# Parquet Page-Skipping Implementation Plan for Iceberg–Spark

**Document Date:** April 28, 2026  
**Scope:** Enabling page-level filtering when reading Parquet files in Iceberg tables via Spark  
**Status:** Design Phase

---

## Executive Summary

This plan outlines a phased approach to implement **Parquet page-skipping** in Iceberg's vectorized Parquet reader. Page-skipping allows Iceberg to skip individual Parquet pages (within a column chunk) that cannot contain rows matching a filter predicate, significantly reducing I/O and decompression overhead compared to row-group-only filtering.

**Key benefits:**
- **Reduced I/O:** Skip entire pages that don't match the filter
- **Lower CPU:** Avoid decompressing pages that won't be read
- **Improved latency:** Especially for point queries or selective range filters
- **Selective impact:** Applies only to small-cardinality columns with suitable statistics

---

## Architecture Overview

### Current State: Row-Group-Level Filtering

The existing filtering pipeline in Iceberg:

```
Parquet.read(file)
  ├─ ReadBuilder.build()
  │   └─ ReadConf (opens ParquetFileReader)
  │       ├─ ParquetMetricsRowGroupFilter (min/max stats per row group)
  │       ├─ ParquetDictionaryRowGroupFilter (dictionary-based filtering)
  │       └─ ParquetBloomRowGroupFilter (bloom filters per row group)
  │
  └─ VectorizedParquetReader (row-group iterator loop)
      └─ FileIterator
          ├─ Skips entire row groups via shouldSkip[] array
          └─ Falls back to page-level for each non-skipped row group
```

**Limitations:**
- Filters are applied at **row-group granularity only**
- A row group may be read entirely even if 99% of its pages don't match the filter
- Page-level statistics exist in Parquet but are unused by Iceberg

---

## Phase 1: Infrastructure & Metadata Layer

### 1.1 Create `ParquetPageFilter` Class

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageFilter.java`

**Purpose:** Evaluate Iceberg expressions against Parquet page statistics; follows the same pattern as `ParquetMetricsRowGroupFilter` but operates on pages instead of row groups.

**Key Methods:**
```java
public class ParquetPageFilter {
  private final Schema schema;
  private final Expression expr;

  // Apply filter to a single page's statistics (via offset index)
  public boolean shouldRead(
      MessageType fileSchema,
      ColumnChunkMetaData columnChunk,
      PageLocation pageLocation,
      Expression filter);

  // Batch evaluate multiple pages
  public boolean[] filterPages(
      MessageType fileSchema,
      ColumnChunkMetaData columnChunk,
      List<PageLocation> pages,
      Expression filter);
}
```

**Design notes:**
- Reuse `BoundExpressionVisitor` logic from `ParquetMetricsRowGroupFilter`
- Extract page statistics via Parquet's `OffsetIndex` (page-level metadata)
- Leverage Apache Parquet's `PageLocation` interface to locate page boundaries
- Handle fallback cases where page stats are unavailable (use "might match" heuristic)

### 1.2 Extract Page Statistics Helper

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageStats.java`

**Purpose:** Utility to safely extract statistics from pages via Parquet metadata APIs.

**Key Methods:**
```java
public class ParquetPageStats {
  // Extract statistics for a specific column in a page
  public static Statistics<?> getPageStats(
      ColumnChunkMetaData columnChunk,
      PageLocation pageLocation,
      OffsetIndex offsetIndex);

  // Get value count for a page
  public static long getPageRowCount(PageLocation pageLocation);

  // Determine if page statistics are available and reliable
  public static boolean hasValidStats(PageLocation pageLocation);
}
```

**Implementation details:**
- Wrap Parquet API calls with proper null checks (not all files have offset indices)
- Cache `OffsetIndex` per column chunk to avoid repeated reads
- Log warnings when page stats are unavailable (not a fatal condition)

---

## Phase 2: Page-Level Skipping in Core Reader

### 2.1 Extend `ReadConf` to Track Page-Level Skips

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java`

**Current state:** Tracks `shouldSkip[]` per row group (boolean array).

**Enhancement:**
```java
public class ReadConf<T> {
  // ...existing code...

  // NEW: Per-column-chunk map of page-skip masks
  private Map<String, boolean[]> pageSkipMasks;

  /**
   * Computes page-skip masks for all projected columns in all non-skipped row groups.
   * Called during ReadConf initialization when filter is present.
   */
  private void computePageSkipMasks(
      MessageType fileSchema,
      Expression filter,
      boolean caseSensitive) {
    this.pageSkipMasks = new LinkedHashMap<>();

    for (int rgIndex = 0; rgIndex < rowGroups.size(); rgIndex++) {
      if (shouldSkip[rgIndex]) continue; // Row group already filtered out

      BlockMetaData rowGroup = rowGroups.get(rgIndex);
      for (ColumnChunkMetaData columnChunk : rowGroup.getColumns()) {
        String columnKey = makeColumnKey(rgIndex, columnChunk.getPath());

        // Get page locations via OffsetIndex (may be null)
        OffsetIndex offsetIndex = reader.getOffsetIndex(rgIndex, columnChunk);
        if (offsetIndex == null) {
          // No page-level metadata; conservatively read all pages
          continue;
        }

        List<PageLocation> pages = offsetIndex.getPageLocations();
        ParquetPageFilter pageFilter = new ParquetPageFilter(schema, filter, caseSensitive);
        boolean[] skipMask = pageFilter.filterPages(fileSchema, columnChunk, pages, filter);
        pageSkipMasks.put(columnKey, skipMask);
      }
    }
  }

  /**
   * Check if a specific page should be skipped.
   *
   * @param rowGroupIndex index of the row group
   * @param columnPath path to the column
   * @param pageIndex index within the column
   * @return true if page should be skipped
   */
  public boolean shouldSkipPage(int rowGroupIndex, ColumnPath columnPath, int pageIndex) {
    String key = makeColumnKey(rowGroupIndex, columnPath);
    boolean[] mask = pageSkipMasks.get(key);
    if (mask == null) {
      return false; // No skip info; read the page
    }
    return pageIndex < mask.length && mask[pageIndex];
  }

  private static String makeColumnKey(int rgIndex, ColumnPath path) {
    return rgIndex + ":" + path.toDotString();
  }
}
```

### 2.2 Integrate Page-Skipping into `ParquetValueReader` Path

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ColumnIterator.java`

**Current state:** Iterates over `PageReadStore` (pages from a row group).

**Enhancement:**
- Add `PageReadStore` wrapper that filters pages before they're read
- Alternatively, modify row-group page loops to consult `ReadConf.shouldSkipPage(...)` before reading each page

**Pseudo-code:**
```java
// In ColumnIterator or downstream value readers
int pageIndex = 0;
while (hasMorePages(pageReadStore)) {
  DataPage page = pageReadStore.readPage();
  
  // NEW: Check if page should be skipped
  if (readConf.shouldSkipPage(currentRowGroupIndex, columnPath, pageIndex)) {
    // Skip reading the data for this page
    log.debug("Skipping page {} for column {}", pageIndex, columnPath);
    pageIndex++;
    continue;
  }

  // Process page as normal
  processPage(page);
  pageIndex++;
}
```

### 2.3 Adapt `VectorizedParquetReader` for Page-Level Filtering

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/VectorizedParquetReader.java`

**Current state:** Uses `PageReadStore` via `VectorizedReader` model interface.

**Enhancement:**
- Modify the batch reading loop to track current page index
- Call `shouldSkipPage(...)` before setting each page on the vectorized model
- Skip pages by not calling `reader.read(...)` or by returning early-end-of-batch markers

**Pseudo-code:**
```java
private static class FileIterator<T> implements CloseableIterator<T> {
  private final ReadConf conf;
  private final VectorizedReader<T> model;
  private int currentRowGroupIndex = 0;
  private int currentPageIndex = 0;
  // ...existing fields...

  @Override
  public T next() {
    if (!hasNext()) throw new NoSuchElementException();

    if (valuesRead >= nextRowGroupStart) {
      advance();
    }

    int numValuesToRead = (int) Math.min(nextRowGroupStart - valuesRead, batchSize);

    // Loop until we find a page that should be read
    boolean pageRead = false;
    while (!pageRead && currentPageIndex < totalPagesInRowGroup) {
      if (!conf.shouldSkipPage(currentRowGroupIndex, columnPath, currentPageIndex)) {
        if (reuseContainers) {
          this.last = model.read(last, numValuesToRead);
        } else {
          this.last = model.read(null, numValuesToRead);
        }
        pageRead = true;
      }
      currentPageIndex++;
    }

    valuesRead += numValuesToRead;
    return last;
  }
}
```

---

## Phase 3: Configuration & Safety Toggles

### 3.1 Add Table Property

**File:** `core/src/main/java/org/apache/iceberg/TableProperties.java`

**New property:**
```java
// Table-level property to enable/disable page-skipping
public static final String PARQUET_PAGE_SKIPPING_ENABLED =
    "iceberg.parquet.page-skipping.enabled";
public static final boolean PARQUET_PAGE_SKIPPING_ENABLED_DEFAULT = false;

// Optional tuning: skip pages only if they save > N bytes
public static final String PARQUET_PAGE_SKIPPING_MIN_BYTES =
    "iceberg.parquet.page-skipping.min-bytes";
public static final long PARQUET_PAGE_SKIPPING_MIN_BYTES_DEFAULT = 10_000; // 10 KB
```

### 3.2 Add Spark Read Configuration

**File:** `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java`

**New methods:**
```java
public boolean parquetPageSkippingEnabled() {
  return confParser
      .booleanConf()
      .option(SparkReadOptions.PARQUET_PAGE_SKIPPING)
      .sessionConf(SparkSQLProperties.PARQUET_PAGE_SKIPPING)
      .tableProperty(TableProperties.PARQUET_PAGE_SKIPPING_ENABLED)
      .defaultValue(TableProperties.PARQUET_PAGE_SKIPPING_ENABLED_DEFAULT)
      .parse();
}

public long parquetPageSkippingMinBytes() {
  return confParser
      .longConf()
      .option(SparkReadOptions.PARQUET_PAGE_SKIPPING_MIN_BYTES)
      .tableProperty(TableProperties.PARQUET_PAGE_SKIPPING_MIN_BYTES)
      .defaultValue(TableProperties.PARQUET_PAGE_SKIPPING_MIN_BYTES_DEFAULT)
      .parse();
}
```

### 3.3 Pass Config Through Read Builder Chain

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java`

**Enhancement to `ReadBuilder`:**
```java
public class ReadBuilder implements InternalData.ReadBuilder {
  private boolean pageSkippingEnabled = false;

  public ReadBuilder pageSkippingEnabled(boolean enabled) {
    this.pageSkippingEnabled = enabled;
    return this;
  }

  @Override
  public <D> CloseableIterable<D> build() {
    // ...existing code...
    if (batchedReaderFunc != null) {
      return new VectorizedParquetReader<>(
          file,
          schema,
          options,
          batchedReaderFunc,
          mapping,
          filter,
          reuseContainers,
          caseSensitive,
          batchSize,
          pageSkippingEnabled);  // NEW parameter
    }
    // ...
  }
}
```

---

## Phase 4: Testing & Validation

### 4.1 Unit Tests

**File:** `parquet/src/test/java/org/apache/iceberg/parquet/TestParquetPageFilter.java`

**Test scenarios:**
1. **Filter on partition-like column** (low cardinality): Verify pages with out-of-range values are skipped
2. **Filter on high-cardinality column**: Verify few/no pages are skipped
3. **AND filters**: Correctness of combining multiple page masks
4. **OR filters:** Correctness of union logic
5. **Mixed operators:** `AND`, `OR`, `NOT`, `IN`, range predicates
6. **Missing statistics:** Verify fallback to conservative "might match"
7. **Disable toggle:** Verify page-skipping doesn't run when disabled

### 4.2 Integration Tests

**File:** `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/data/vectorized/parquet/TestParquetPageSkipping.java`

**Test scenarios:**
1. **Spark vectorized reads** with page-skipping enabled/disabled
2. **Performance comparison:** Measure I/O reduction (bytes read before/after)
3. **Correctness:** Verify results match non-pageskipped reads
4. **Edge cases:** Empty row groups, single-page row groups, files without offset indices
5. **Multi-column projection:** Page-skipping on one column doesn't break others

### 4.3 Benchmark

**File:** `spark/v4.1/spark/src/jmh/java/org/apache/iceberg/spark/data/parquet/PageSkippingBenchmark.java`

**Benchmarks:**
1. Point lookups on timestamp column (high selectivity)
2. Range queries on numeric column (medium selectivity)
3. Large sequential scans (low selectivity, minimal page-skipping benefit)
4. Mixed workloads (payload column unprojected, filtering on ID column)

---

## Phase 5: Documentation & Release

### 5.1 User Documentation

**File:** `docs/docs/spark-configuration.md`

**Sections to add:**
- Feature overview: "Parquet Page-Skipping"
- Table property: `iceberg.parquet.page-skipping.enabled`
- Configuration: `spark.sql.iceberg.parquet.page-skipping.enabled`
- Performance impact: when to enable
- Limitations: requires offset indices, older Parquet files may not have them

### 5.2 Release Notes

**Highlight:**
- New opt-in feature: Parquet page-level filtering
- Expected I/O reduction: case-dependent (10–80% for selective queries)
- No breaking changes; defaults to `false`

---

## Implementation Checklist

- [ ] **Phase 1: Infrastructure**
  - [ ] Create `ParquetPageFilter` class
  - [ ] Create `ParquetPageStats` utility
  - [ ] Handle Parquet API compatibility (OffsetIndex availability)

- [ ] **Phase 2: Core Reader**
  - [ ] Extend `ReadConf` with page-skip mask computation
  - [ ] Integrate page-skipping into `ColumnIterator` loop
  - [ ] Adapt `VectorizedParquetReader` for page-level tracking
  - [ ] Handle fallback (row-based readers if needed)

- [ ] **Phase 3: Configuration**
  - [ ] Add `TableProperties` entries
  - [ ] Add `SparkReadConf` and `SparkReadOptions` methods
  - [ ] Thread config through `Parquet.ReadBuilder`

- [ ] **Phase 4: Testing**
  - [ ] Unit tests for `ParquetPageFilter`
  - [ ] Integration tests for end-to-end page-skipping
  - [ ] Correctness validation (results = non-skipped results)
  - [ ] JMH benchmarks

- [ ] **Phase 5: Documentation**
  - [ ] User guide: when/how to enable page-skipping
  - [ ] Configuration reference
  - [ ] Release notes
  - [ ] Architectural decision record (ADR) if needed

---

## Design Considerations

### Safe Defaults
- Page-skipping is **disabled by default** (`false`)
- Minimal performance penalty if disabled
- Users explicitly opt-in for production use

### Fallback Behavior
- If page statistics unavailable → read all pages (conservative)
- If `OffsetIndex` missing → skip page-level filtering silently
- No errors or exceptions; graceful degradation

### Interaction with Existing Filters
- Page-skipping **complements** (not replaces) row-group filtering
- Both layers applied: first row groups, then pages within selected row groups
- Filter union: a page is skipped only if ALL filter terms reject it

### Supported Column Types
- Phase 1: Primitives (int, long, float, double, boolean, string/binary)
- Nested types: deferred (require recursive filter evaluation)
- Complex predicates: AND, OR, NOT, comparison, IN

### Performance Profile
- **Scanning time:** Offset index lookup + O(pages) filter evaluations ≈ 1–5 ms per row group
- **I/O savings:** High for selective filters (10–80% depending on data distribution)
- **Cost:** Minimal if filter is unselective (few pages skipped)

### Multi-Engine Compatibility
- Implementation in `iceberg-parquet` (engine-agnostic)
- Spark integration via `VectorizedSparkParquetReaders`
- Flink could reuse same infrastructure (future)

---

## Known Limitations & Future Work

### Phase 1 Scope
- **No support** for nested type filtering at page level
- **No support** for variant types
- Assumes homogeneous page statistics (acceptable for correctness, may miss optimization opportunities)

### Future Enhancements
1. **Page-level bloom filters** (Parquet v2+ feature)
2. **Columnar statistics reuse** across reads (cache offset indices)
3. **Adaptive page skipping** (estimate I/O savings before materializing)
4. **Nested type support** (recursive page evaluation)
5. **Page-aware vectorization** (batch size tuning based on page selectivity)

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Incorrect page stats lead to missing rows | Conservative fallback; unit test on edge cases |
| Performance regression in unselective queries | Benchmarks catch regressions; default off |
| Parquet compatibility issues | Target Parquet 1.12+ (OffsetIndex standard); graceful fallback for older files |
| Interaction with encryption/compression | Test with encrypted pages; compression transparent to page filtering |
| Memory overhead (page masks) | One boolean per page; negligible for typical row groups (<10k pages per RG) |

---

## Timeline & Effort Estimate

| Phase | Effort | Duration |
|-------|--------|----------|
| Phase 1: Infrastructure | 2–3 weeks | Week 1–3 |
| Phase 2: Core Reader | 3–4 weeks | Week 4–7 |
| Phase 3: Configuration | 1 week | Week 8 |
| Phase 4: Testing | 3–4 weeks | Week 9–12 |
| Phase 5: Documentation | 1 week | Week 13 |
| **Total** | **10–13 weeks** | **~3 months** |

**Parallelization opportunity:** Phase 1 and start of Phase 4 (unit tests) can overlap.

---

## Appendix: Parquet Offset Index API Reference

### Key Classes
- `org.apache.parquet.hadoop.ParquetFileReader#getOffsetIndex(int, ColumnChunkMetaData)`
- `org.apache.parquet.hadoop.metadata.OffsetIndex` (list of `PageLocation`)
- `org.apache.parquet.hadoop.metadata.PageLocation#getFirstRowIndex(), getLastRowIndex()`

### Page Statistics Extraction
- `PageLocation.getFirstRowIndex()` / `.getLastRowIndex()` give row ranges per page
- Column statistics referenced via `ColumnChunkMetaData`
- Note: Page-level _statistics_ (min/max) require custom parsing; `PageLocation` gives row ranges only

### Compatibility
- Available in Parquet since 1.12.0
- Iceberg currently targets Parquet 1.12+ (verify version in `build.gradle`)
- Graceful fallback if `getOffsetIndex()` returns `null`


