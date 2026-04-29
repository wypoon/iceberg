# Parquet Page-Skipping: Architecture & Code Sketches

**Companion to:** `PARQUET_PAGE_SKIPPING_PLAN.md`  
**Date:** April 28, 2026

---

## 1. High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Spark SQL Query                          │
│              (Filter: WHERE timestamp > X)                  │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────▼──────────────┐
        │  SparkScanBuilder          │
        │  .filter(expression)       │
        └─────────────┬──────────────┘
                      │
        ┌─────────────▼──────────────────────────────┐
        │  SparkBatch.createReaderFactory()         │
        │  (Chooses vectorized vs. row reader)       │
        └─────────────┬──────────────────────────────┘
                      │
        ┌─────────────▼────────────────────────────────────┐
        │  SparkColumnarReaderFactory                      │
        │  .createColumnarReader(InputPartition)          │
        └─────────────┬────────────────────────────────────┘
                      │
        ┌─────────────▼────────────────────────────────────┐
        │  BatchDataReader.open(FileScanTask)              │
        │  .newBatchIterable(...)                          │
        └─────────────┬────────────────────────────────────┘
                      │
        ┌─────────────▼────────────────────────────────────┐
        │  FormatModelRegistry.readBuilder(...)            │
        │  (Routes to VectorizedSparkParquetReaders)       │
        └─────────────┬────────────────────────────────────┘
                      │
        ┌─────────────▼────────────────────────────────────┐
        │  Parquet.read(file).createBatchedReaderFunc(...) │
        │  .pageSkippingEnabled(config)       [NEW]        │
        │  .build()                                         │
        └─────────────┬────────────────────────────────────┘
                      │
        ┌─────────────▼──────────────────────────────────────────┐
        │  VectorizedParquetReader                               │
        │  - Opens ParquetFileReader                             │
        │  - ReadConf.computePageSkipMasks(...) [NEW]           │
        │    ├─ ParquetMetricsRowGroupFilter (existing)         │
        │    ├─ ParquetDictionaryRowGroupFilter (existing)      │
        │    ├─ ParquetBloomRowGroupFilter (existing)           │
        │    └─ ParquetPageFilter.filterPages(...) [NEW]        │
        │  - FileIterator.next()                                 │
        │    ├─ Advances to next non-skipped row group          │
        │    ├─ Tracks currentPageIndex [NEW]                   │
        │    └─ Calls model.read() for non-skipped pages [NEW]  │
        └─────────────┬──────────────────────────────────────────┘
                      │
        ┌─────────────▼──────────────────────────────────────┐
        │  ColumnarBatchReader (Arrow/Spark vectors)        │
        │  Returns ColumnarBatch                             │
        └─────────────┬──────────────────────────────────────┘
                      │
                      ▼
            ╔═════════════════════╗
            ║   Spark Executor    ║
            ║  (Process batch)    ║
            ╚═════════════════════╝
```

---

## 2. Class Diagram: New Components

```
┌────────────────────────────────────────┐
│     ParquetPageFilter                  │
├────────────────────────────────────────┤
│ - schema: Schema                       │
│ - expr: Expression (bound)             │
├────────────────────────────────────────┤
│ + shouldRead(fileSchema, colChunk,     │
│      pageLocation, filter): boolean    │
│ + filterPages(fileSchema, colChunk,    │
│      pages[], filter): boolean[]       │
│ - eval(fileSchema, ...) : visitor      │
└────────────────────────────────────────┘
         │ uses
         │
         ▼
┌────────────────────────────────────────┐
│     ParquetPageStats                   │
├────────────────────────────────────────┤
│ + getPageStats(...): Statistics        │
│ + getPageRowCount(...): long           │
│ + hasValidStats(...): boolean          │
│ - extractFromOffsetIndex(...) [util]   │
└────────────────────────────────────────┘

┌────────────────────────────────────────────┐
│     ReadConf (EXTENDED)                    │
├────────────────────────────────────────────┤
│ - pageSkipMasks: Map<String, boolean[]>    │
│ - pageSkippingEnabled: boolean             │
├────────────────────────────────────────────┤
│ + computePageSkipMasks(schema, filter)     │
│ + shouldSkipPage(rgIdx, path, pageIdx): ✓ │
│ - makeColumnKey(...): String [util]        │
└────────────────────────────────────────────┘

┌────────────────────────────────────────┐
│   VectorizedParquetReader (EXTENDED)   │
├────────────────────────────────────────┤
│ - pageSkippingEnabled: boolean         │
├────────────────────────────────────────┤
│   FileIterator:                        │
│   - currentPageIndex: int [NEW]        │
│   - pageSkipMask: boolean[] [NEW]      │
│   + next(): T [modified logic]         │
└────────────────────────────────────────┘

┌─────────────────────────────────────┐
│   Parquet.ReadBuilder (EXTENDED)    │
├─────────────────────────────────────┤
│ - pageSkippingEnabled: boolean       │
├─────────────────────────────────────┤
│ + pageSkippingEnabled(bool)          │
│ # build() [passes flag to reader]    │
└─────────────────────────────────────┘
```

---

## 3. Code Sketch: ParquetPageFilter

```java
// parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageFilter.java

package org.apache.iceberg.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter pages in a Parquet column chunk based on Iceberg filter expressions.
 * Similar to ParquetMetricsRowGroupFilter but operates at page granularity.
 */
public class ParquetPageFilter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetPageFilter.class);

  private final Schema schema;
  private final Expression expr;

  public ParquetPageFilter(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public ParquetPageFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    this.schema = schema;
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, Expressions.rewriteNot(unbound), caseSensitive);
  }

  /**
   * Determine which pages in a column chunk should be skipped.
   *
   * @param fileSchema Parquet message type
   * @param columnChunk metadata for the column
   * @param pageLocations list of page locations with row ranges
   * @return boolean array where true indicates page should be skipped
   */
  public boolean[] filterPages(
      MessageType fileSchema,
      ColumnChunkMetaData columnChunk,
      List<PageLocation> pageLocations) {
    
    boolean[] skipMask = new boolean[pageLocations.size()];
    
    if (pageLocations.isEmpty()) {
      return skipMask;
    }

    // Extract column descriptor and type
    ColumnPath colPath = columnChunk.getPath();
    PrimitiveType colType = fileSchema.getType(colPath.toArray()).asPrimitiveType();
    if (colType.getId() == null) {
      // No field ID; can't correlate with Iceberg schema
      return skipMask;
    }

    int fieldId = colType.getId().intValue();
    Type icebergType = schema.findType(fieldId);
    if (icebergType == null) {
      return skipMask;
    }

    // Evaluate filter for each page
    for (int pageIdx = 0; pageIdx < pageLocations.size(); pageIdx++) {
      PageLocation pageLoc = pageLocations.get(pageIdx);
      skipMask[pageIdx] = shouldSkipPage(pageIdx, pageLoc, columnChunk, colType, icebergType);
    }

    return skipMask;
  }

  private boolean shouldSkipPage(
      int pageIdx,
      PageLocation pageLoc,
      ColumnChunkMetaData columnChunk,
      PrimitiveType colType,
      Type icebergType) {
    
    // Note: PageLocation provides row ranges but not statistics directly.
    // We'd need to use Parquet's OffsetIndex to fetch page-specific stats.
    // For now, this is a simplified sketch; real impl will need OffsetIndex support.
    
    try {
      Statistics<?> pageStats = ParquetPageStats.getPageStats(columnChunk, pageIdx);
      if (pageStats == null || pageStats.isEmpty()) {
        // No stats available; conservatively read page
        return false;
      }

      // Use same visitor logic as ParquetMetricsRowGroupFilter
      PageFilterVisitor visitor = new PageFilterVisitor(schema, icebergType, colType);
      Boolean result = ExpressionVisitors.visit(expr, visitor);
      
      // If result is null (unhandled), be conservative
      return result == Boolean.FALSE; // skip only if definitely doesn't match
    } catch (Exception e) {
      LOG.warn("Failed to evaluate page filter for column {}, page {}", colType.getName(), pageIdx, e);
      return false; // On error, read the page
    }
  }

  /**
   * Visitor that evaluates expressions against page statistics.
   * Reuses logic from ParquetMetricsRowGroupFilter.
   */
  private static class PageFilterVisitor 
      extends ExpressionVisitors.BoundExpressionVisitor<Boolean> {
    // Similar to ParquetMetricsRowGroupFilter.MetricsEvalVisitor
    // Implement lt, gt, eq, etc. using page statistics
    
    private final Schema schema;
    private final Type icebergType;
    private final PrimitiveType colType;
    
    PageFilterVisitor(Schema schema, Type icebergType, PrimitiveType colType) {
      this.schema = schema;
      this.icebergType = icebergType;
      this.colType = colType;
    }

    @Override
    public Boolean alwaysTrue() {
      return true; // all pages might match
    }

    @Override
    public Boolean alwaysFalse() {
      return false; // no pages match
    }

    // ... implement predicate methods (eq, lt, gt, in, etc.) ...
    // Similar to ParquetMetricsRowGroupFilter
  }
}
```

---

## 4. Code Sketch: ReadConf Extension

```java
// parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java
// (Simplified excerpt showing page-skipping logic)

public class ReadConf<T> {
  private final ParquetFileReader reader;
  private final List<BlockMetaData> rowGroups;
  private final boolean[] shouldSkip; // existing row-group skip mask
  private Map<String, boolean[]> pageSkipMasks; // NEW
  private final boolean pageSkippingEnabled; // NEW

  @SuppressWarnings("unchecked")
  ReadConf(
      InputFile file,
      ParquetReadOptions options,
      Schema expectedSchema,
      Expression filter,
      Function<MessageType, ParquetValueReader<?>> readerFunc,
      Function<MessageType, VectorizedReader<?>> batchedReaderFunc,
      NameMapping nameMapping,
      boolean reuseContainers,
      boolean caseSensitive,
      Integer batchSize,
      boolean pageSkippingEnabled) { // NEW parameter
    
    this.file = file;
    this.options = options;
    this.reader = newReader(file, options);
    this.pageSkippingEnabled = pageSkippingEnabled;
    
    MessageType fileSchema = reader.getFileMetaData().getSchema();
    // ... existing schema handling ...

    this.rowGroups = reader.getRowGroups();
    this.shouldSkip = new boolean[rowGroups.size()];
    this.pageSkipMasks = new LinkedHashMap<>(); // NEW

    // Apply row-group level filtering
    ParquetMetricsRowGroupFilter statsFilter = null;
    ParquetDictionaryRowGroupFilter dictFilter = null;
    ParquetBloomRowGroupFilter bloomFilter = null;
    
    if (filter != null) {
      statsFilter = new ParquetMetricsRowGroupFilter(expectedSchema, filter, caseSensitive);
      dictFilter = new ParquetDictionaryRowGroupFilter(expectedSchema, filter, caseSensitive);
      bloomFilter = new ParquetBloomRowGroupFilter(expectedSchema, filter, caseSensitive);
    }

    long computedTotalValues = 0L;
    for (int rgIdx = 0; rgIdx < shouldSkip.length; rgIdx++) {
      BlockMetaData rowGroup = rowGroups.get(rgIdx);
      boolean shouldRead = 
          filter == null
              || (statsFilter.shouldRead(fileSchema, rowGroup)
                  && dictFilter.shouldRead(fileSchema, rowGroup, reader.getDictionaryReader(rowGroup))
                  && bloomFilter.shouldRead(fileSchema, rowGroup, reader.getBloomFilterDataReader(rowGroup)));
      this.shouldSkip[rgIdx] = !shouldRead;
      if (shouldRead) {
        computedTotalValues += rowGroup.getRowCount();
      }
    }

    this.totalValues = computedTotalValues;

    // NEW: Compute page-skip masks for non-row-group-skipped rows
    if (pageSkippingEnabled && filter != null) {
      computePageSkipMasks(fileSchema, expectedSchema, filter, caseSensitive);
    }

    // ... rest of existing logic ...
  }

  /**
   * Compute page-skip masks for all projected columns in non-skipped row groups.
   */
  private void computePageSkipMasks(
      MessageType fileSchema,
      Schema expectedSchema,
      Expression filter,
      boolean caseSensitive) {
    
    ParquetPageFilter pageFilter = new ParquetPageFilter(expectedSchema, filter, caseSensitive);

    for (int rgIdx = 0; rgIdx < rowGroups.size(); rgIdx++) {
      if (shouldSkip[rgIdx]) {
        continue; // Row group already filtered out
      }

      BlockMetaData rowGroup = rowGroups.get(rgIdx);
      for (ColumnChunkMetaData columnChunk : rowGroup.getColumns()) {
        ColumnPath colPath = columnChunk.getPath();
        
        // Get offset index (may be null for older files)
        OffsetIndex offsetIndex = null;
        try {
          offsetIndex = reader.getOffsetIndex(rgIdx, columnChunk);
        } catch (Exception e) {
          // Offset index not available; skip page-level filtering for this column
          LOG.debug("No offset index available for column {}, row group {}", colPath, rgIdx);
          continue;
        }

        if (offsetIndex == null || offsetIndex.getPageLocations().isEmpty()) {
          continue;
        }

        List<PageLocation> pageLocations = offsetIndex.getPageLocations();
        boolean[] skipMask = pageFilter.filterPages(fileSchema, columnChunk, pageLocations);
        
        String columnKey = makeColumnKey(rgIdx, colPath);
        pageSkipMasks.put(columnKey, skipMask);
      }
    }
  }

  /**
   * Check if a specific page should be skipped.
   */
  public boolean shouldSkipPage(int rowGroupIndex, ColumnPath columnPath, int pageIndex) {
    if (!pageSkippingEnabled) {
      return false; // Feature disabled
    }

    String key = makeColumnKey(rowGroupIndex, columnPath);
    boolean[] mask = pageSkipMasks.get(key);
    
    if (mask == null) {
      return false; // No skip mask for this column
    }

    return pageIndex < mask.length && mask[pageIndex];
  }

  private static String makeColumnKey(int rgIndex, ColumnPath path) {
    return rgIndex + ":" + path.toDotString();
  }

  // ... rest of existing methods ...
}
```

---

## 5. Code Sketch: VectorizedParquetReader Integration

```java
// parquet/src/main/java/org/apache/iceberg/parquet/VectorizedParquetReader.java
// (Simplified excerpt showing page-skipping integration)

public class VectorizedParquetReader<T> extends CloseableGroup 
    implements CloseableIterable<T> {
  
  private final boolean pageSkippingEnabled; // NEW

  public VectorizedParquetReader(
      InputFile input,
      Schema expectedSchema,
      ParquetReadOptions options,
      Function<MessageType, VectorizedReader<?>> readerFunc,
      NameMapping nameMapping,
      Expression filter,
      boolean reuseContainers,
      boolean caseSensitive,
      int maxRecordsPerBatch,
      boolean pageSkippingEnabled) { // NEW parameter
    
    this.input = input;
    this.expectedSchema = expectedSchema;
    this.options = options;
    this.batchReaderFunc = readerFunc;
    this.filter = filter == Expressions.alwaysTrue() ? null : filter;
    this.reuseContainers = reuseContainers;
    this.caseSensitive = caseSensitive;
    this.batchSize = maxRecordsPerBatch;
    this.nameMapping = nameMapping;
    this.pageSkippingEnabled = pageSkippingEnabled; // NEW
  }

  @Override
  public CloseableIterator<T> iterator() {
    FileIterator<T> iter = new FileIterator<>(init(), pageSkippingEnabled);
    addCloseable(iter);
    return iter;
  }

  private static class FileIterator<T> implements CloseableIterator<T> {
    private final ParquetFileReader reader;
    private final VectorizedReader<T> model;
    private final ReadConf conf;
    private final boolean pageSkippingEnabled;
    
    // NEW: page-level tracking
    private int currentRowGroupIndex = 0;
    private int currentPageIndex = 0;
    private ColumnPath currentColumnPath = null;

    // ... existing fields ...

    FileIterator(ReadConf conf, boolean pageSkippingEnabled) {
      this.conf = conf;
      this.pageSkippingEnabled = pageSkippingEnabled;
      // ... existing init ...
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (valuesRead >= nextRowGroupStart) {
        advance();
      }

      int numValuesToRead = (int) Math.min(nextRowGroupStart - valuesRead, batchSize);

      // NEW: If page-skipping enabled, read pages while skipping filtered ones
      if (pageSkippingEnabled) {
        return readNextNonSkippedPage(numValuesToRead);
      } else {
        // Existing logic: just read next page
        if (reuseContainers) {
          this.last = model.read(last, numValuesToRead);
        } else {
          this.last = model.read(null, numValuesToRead);
        }
        valuesRead += numValuesToRead;
        return last;
      }
    }

    /**
     * Read data from the next non-skipped page.
     * If current page is skipped, advance to next page and retry.
     */
    private T readNextNonSkippedPage(int numValuesToRead) {
      // Note: This is a simplified sketch. Real implementation would need
      // to track pages within PageReadStore more carefully.
      
      boolean pageRead = false;
      while (!pageRead && currentPageIndex < totalPagesInCurrentRowGroup) {
        
        if (!conf.shouldSkipPage(currentRowGroupIndex, currentColumnPath, currentPageIndex)) {
          // Read this page
          if (reuseContainers) {
            this.last = model.read(last, numValuesToRead);
          } else {
            this.last = model.read(null, numValuesToRead);
          }
          pageRead = true;
        }
        // Else: page is skipped, just increment and try next
        
        currentPageIndex++;
      }

      if (!pageRead) {
        // All remaining pages in row group are skipped; shouldn't happen if filter
        // was correct, but be defensive
        throw new NoSuchElementException("Ran out of pages");
      }

      valuesRead += numValuesToRead;
      return last;
    }

    private void advance() {
      // Skip to next non-skipped row group
      while (shouldSkip[nextRowGroup]) {
        nextRowGroup += 1;
        reader.skipNextRowGroup();
      }

      PageReadStore pages = reader.readNextRowGroup();
      model.setRowGroupInfo(pages, columnChunkMetadata.get(nextRowGroup));
      nextRowGroupStart += pages.getRowCount();
      nextRowGroup += 1;

      // NEW: Reset page tracking
      currentRowGroupIndex = nextRowGroup - 1;
      currentPageIndex = 0;
    }

    @Override
    public void close() throws IOException {
      model.close();
      reader.close();
    }
  }
}
```

---

## 6. Configuration Integration

### Parquet.ReadBuilder Changes

```java
// parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java
// (ReadBuilder excerpt)

public class Parquet {
  public static class ReadBuilder {
    private boolean pageSkippingEnabled = false;

    public ReadBuilder pageSkippingEnabled(boolean enabled) {
      this.pageSkippingEnabled = enabled;
      return this;
    }

    @Override
    public <D> CloseableIterable<D> build() {
      // ... existing setup ...

      if (createWriterFunc != null) {
        // Vectorized path
        return new org.apache.iceberg.parquet.ParquetWriter<>(
            // ... existing params ...
            pageSkippingEnabled); // NEW
      } else {
        // Row-based path
        ParquetReadBuilder<D> parquetReadBuilder = 
            new ParquetReadBuilder<D>(ParquetIO.file(file))
                // ... existing config ...
                .pageSkippingEnabled(pageSkippingEnabled) // NEW
                .build();
        return new ParquetIterable<>(parquetReadBuilder);
      }
    }
  }
}
```

### Spark Config Integration

```java
// spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/BaseBatchReader.java
// (excerpt)

protected CloseableIterable<ColumnarBatch> newBatchIterable(
    InputFile inputFile,
    FileFormat format,
    long start,
    long length,
    Expression residual,
    Map<Integer, ?> idToConstant,
    SparkDeleteFilter deleteFilter) {
  
  ReadBuilder<ColumnarBatch, ?> readBuilder =
      FormatModelRegistry.readBuilder(format, ColumnarBatch.class, inputFile);

  // NEW: Set page-skipping flag from config
  boolean pageSkippingEnabled = /* read from readConf */;
  if (readBuilder instanceof ParquetReadBuilder) {
    ((ParquetReadBuilder<?>) readBuilder).pageSkippingEnabled(pageSkippingEnabled);
  }

  CloseableIterable<ColumnarBatch> iterable =
      readBuilder
          .project(deleteFilter.requiredSchema())
          // ... rest of config ...
          .build();

  return CloseableIterable.transform(iterable, new BatchDeleteFilter(deleteFilter)::filterBatch);
}
```

---

## 7. Test Sketch

```java
// parquet/src/test/java/org/apache/iceberg/parquet/TestParquetPageFilter.java

public class TestParquetPageFilter {
  
  @Test
  public void testFilterLowCardinalityColumn() {
    // Create Parquet file with timestamp column (1M rows, 100 pages)
    // Page 1-50: timestamps 2024-01-01
    // Page 51-100: timestamps 2024-12-31
    
    // Apply filter: timestamp > 2024-06-01
    // Expected: pages 51-100 match, pages 1-50 should be skipped
    
    ParquetPageFilter filter = new ParquetPageFilter(schema, filterExpr);
    boolean[] skipMask = filter.filterPages(fileSchema, columnChunk, pageLocations);
    
    for (int i = 0; i < 50; i++) {
      assertThat(skipMask[i]).isTrue(); // Should skip
    }
    for (int i = 50; i < 100; i++) {
      assertThat(skipMask[i]).isFalse(); // Should read
    }
  }

  @Test
  public void testHighCardinalityFallbackToRead() {
    // Parquet file with random ID column (high cardinality)
    // Filter: id IN (42, 99, 1000, ...)
    // Expected: pages might match (conservative); don't skip
    
    boolean[] skipMask = filter.filterPages(fileSchema, columnChunk, pageLocations);
    for (boolean skip : skipMask) {
      assertThat(skip).isFalse(); // Conservative: read all
    }
  }

  @Test
  public void testMissingOffsetIndex() {
    // File without offset index
    // Expected: graceful fallback; page-skipping disabled silently
    
    ReadConf conf = new ReadConf(..., pageSkippingEnabled = true);
    // No exception; just reads all pages
  }

  @Test
  public void testVectorizedReadCorrectness() {
    // Create file with 10M rows, 1000 pages
    // ReadBuilder with pageSkippingEnabled=true + filter
    
    try (CloseableIterable<ColumnarBatch> result = 
        Parquet.read(file)
            .project(schema)
            .createBatchedReaderFunc(readerFunc)
            .pageSkippingEnabled(true)
            .filter(filterExpr)
            .build()) {
      
      // Read batches and collect rows
      List<Row> resultRows = collectRows(result);
      
      // Verify result matches non-pageskipped read
      try (CloseableIterable<ColumnarBatch> baseline =
          Parquet.read(file)
              .project(schema)
              .createBatchedReaderFunc(readerFunc)
              .pageSkippingEnabled(false)
              .filter(filterExpr)
              .build()) {
        
        List<Row> baselineRows = collectRows(baseline);
        assertThat(resultRows).isEqualTo(baselineRows);
      }
    }
  }
}
```

---

## 8. Performance Estimation

### Benchmark Scenarios

```
Scenario: Point lookup on timestamp (uniform distribution)

Data: 100 GB Parquet file
Row Groups: 1,000 (100 MB each)
Pages per RG: 100 (1 MB each)
Filter: timestamp = '2024-06-15'

Without Page-Skipping:
├─ Row-group filter: Skips ~950 RGs (fast, metrics-based)
├─ Remaining RGs: 50 (5 GB total)
└─ I/O: 5 GB read, 5 GB decompressed, processed

With Page-Skipping:
├─ Row-group filter: Same 50 RGs (5 GB)
├─ Page filter: Each RG has ~10 matching pages (99 pages skipped/RG)
│  (Timestamp usually clustered or well-separated by page)
├─ I/O: ~500 MB read (5 GB - 95%)
└─ I/O savings: ~95%, Latency: 10-20x faster
```

---

## 9. Rollout Strategy

### Pilot Release (Iceberg 1.8.0)
- Feature flag: `PARQUET_PAGE_SKIPPING_ENABLED_DEFAULT = false`
- Documentation: "Experimental feature"
- Monitoring: Metrics on page-skip rate, I/O savings

### Stabilization (Iceberg 1.9.0)
- Bug fixes based on pilot feedback
- Performance tuning (cache offset indices)
- Consider default: `false` still (conservative)

### General Availability (Iceberg 1.10.0)
- Widely tested; consider enabling by default for new tables
- Full documentation and best practices guide

---

## 10. References

- Apache Parquet Specification: [Offset Index](https://github.com/apache/parquet-format/blob/master/PageIndex.md)
- Iceberg Architecture: [Parquet Reader Design](docs/design/parquet-reader.md)
- Parquet Java API: `org.apache.parquet.hadoop.metadata`


