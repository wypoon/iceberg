# Parquet Page-Skipping: Non-Vectorized Read Path Implementation

**Date:** April 28, 2026  
**Status:** Addendum to Main Plan  
**Scope:** Detailed implementation for row-based (non-vectorized) readers

---

## Executive Summary

The main plan focused on vectorized reads (`VectorizedParquetReader`). However, **non-vectorized reads** (`ParquetReader` → row-based path) are equally important:

- **Used when:** Vectorization is disabled or unsupported (nested types, complex projections)
- **Affected components:** `ParquetReader.FileIterator`, `ParquetValueReader`, `ColumnIterator`
- **Implementation differs from vectorized path:** Row-by-row iteration instead of batch iteration
- **Complexity:** Higher (requires page-level control in row readers, single-row overhead)

This document provides the **missing implementation details** for page-skipping in non-vectorized reads.

---

## 1. Architecture: Non-Vectorized Read Path

### Current Flow (Row-by-Row)

```
ParquetReader.iterator()
  └─ FileIterator (row-based loop)
      ├─ hasNext() → valuesRead < totalValues
      ├─ next()
      │   ├─ Call model.read(last) one row at a time
      │   ├─ If valuesRead >= nextRowGroupStart → advance()
      │   └─ Return single row
      │
      └─ advance()
          ├─ Skip row groups via shouldSkip[]
          ├─ reader.readNextRowGroup() → PageReadStore
          ├─ model.setPageSource(pages)
          └─ pages delivered to ParquetValueReader (column iterators)

ParquetValueReader (generic row container)
  └─ Internal: delegates to ColumnIterator per column

ColumnIterator (per column)
  ├─ Iterates through pages in PageReadStore
  ├─ Reads definition/repetition levels
  ├─ Decodes values intelligently per column
  └─ Returns column value for current row
```

### Key Difference from Vectorized

- **Vectorized:** Batches of rows → page-skipping can skip entire pages
- **Non-vectorized:** One row at a time → must still **decompand all pages**, even if row values will be skipped

**Challenge:** Page-skipping at row level is less effective because:
1. Reader still reads page headers and row-group metadata
2. Decompression happens per page (can't skip entirely)
3. Definition/repetition levels must be decoded

**Solution:** Skip pages at the `ColumnIterator` level; decode row-level skip masks upfront.

---

## 2. Implementation: ReadConf (Row-Based Version)

The row-based path uses `ParquetReader`, which calls `ReadConf` the same way as vectorized, but the skip logic is applied differently.

### Current ReadConf Constructor (Row-Based)

```java
// In ReadConf, when readerFunc != null (row-based reader):
ReadConf<T> readConf = new ReadConf<>(
    inputFile,
    options,
    expectedSchema,
    filter,
    readerFunc,        // ParquetValueReader factory
    null,              // batchedReaderFunc (null for row-based)
    nameMapping,
    reuseContainers,
    caseSensitive,
    null);             // batchSize (null for row-based)
```

### Enhancement for Page-Skipping (Row-Based)

The **same page-skip mask computation** happens in `ReadConf.computePageSkipMasks()`, but the **usage location** is different.

**Key insight:** Instead of readConf computing page-skip masks and VectorizedParquetReader consuming them, we create a **wrapper around PageReadStore** that filters pages.

```java
// NEW: In ReadConf (row-based path)
public class ReadConf<T> {
  // ...existing fields...
  private Map<String, boolean[]> pageSkipMasks; // NEW (same as vectorized)
  private boolean pageSkippingEnabled; // NEW

  // Existing constructor signature
  ReadConf<T> readConf = new ReadConf<>(
      file, options, expectedSchema, filter,
      readerFunc,  // row-based
      null,        // batchedReaderFunc = null
      nameMapping, reuseContainers, caseSensitive,
      null,        // batchSize
      pageSkippingEnabled);  // NEW parameter - same as vectorized

  // NEW: For row-based path, expose page-skip masks
  public Map<String, boolean[]> pageSkipMasks() {
    return pageSkipMasks;  // Computed same way as vectorized
  }

  // NEW: Query whether a page should be skipped (same as vectorized)
  public boolean shouldSkipPage(int rowGroupIndex, ColumnPath columnPath, int pageIndex) {
    if (!pageSkippingEnabled) {
      return false;
    }
    String key = makeColumnKey(rowGroupIndex, columnPath);
    boolean[] mask = pageSkipMasks.get(key);
    if (mask == null) {
      return false;  // No mask = read page
    }
    return pageIndex < mask.length && mask[pageIndex];
  }
}
```

**No change needed** beyond what's already in the plan for `computePageSkipMasks()`. The same method works for both row-based and vectorized paths.

---

## 3. Implementation: PageReadStoreWrapper (NEW)

The **critical new component** for row-based page-skipping: wrap `PageReadStore` to filter pages before they reach `ColumnIterator`.

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/FilteredPageReadStore.java` (NEW)

```java
package org.apache.iceberg.parquet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.column.page.Pages;

/**
 * Wraps a PageReadStore and filters pages per column based on skip masks.
 * Row-based readers iterate through pages one-by-one; this wrapper prevents
 * ColumnIterator from seeing pages that should be skipped.
 */
public class FilteredPageReadStore implements PageReadStore {
  private final PageReadStore delegate;
  private final Map<ColumnPath, boolean[]> pageSkipMasks;
  private final Map<ColumnPath, Integer> pageIndexTracking;

  public FilteredPageReadStore(
      PageReadStore delegate,
      Map<ColumnPath, boolean[]> pageSkipMasks) {
    this.delegate = delegate;
    this.pageSkipMasks = pageSkipMasks;
    this.pageIndexTracking = new HashMap<>();
  }

  /**
   * For each column, iterate through pages and skip those marked in skip masks.
   * When ColumnIterator calls getPageReader(columnDescriptor), return a reader
   * that skips filtered-out pages.
   */
  @Override
  public org.apache.parquet.column.ColumnReadStoreImpl getColumnReadStore(
      org.apache.parquet.column.page.PageReadStore.ColumnReadStoreFactory factory) {
    // Delegate column read stores; filtering happens at page level
    return delegate.getColumnReadStore(factory);
  }

  @Override
  public Pages getPages(ColumnPath path, long rowCount, ... ) {
    // Get pages from delegate
    Pages originalPages = delegate.getPages(path, rowCount, ...);

    if (!pageSkipMasks.containsKey(path)) {
      // No skip mask for this column; return all pages
      return originalPages;
    }

    // Filter out skipped pages
    boolean[] skipMask = pageSkipMasks.get(path);
    List<DataPage> filteredPages = new ArrayList<>();
    int pageIdx = 0;

    for (DataPage page : originalPages) {
      if (pageIdx < skipMask.length && !skipMask[pageIdx]) {
        // Page should NOT be skipped; include it
        filteredPages.add(page);
      }
      // else: page is skipped; don't include
      pageIdx++;
    }

    return new Pages(filteredPages);  // Return filtered list
  }

  // ... delegate all other methods to delegate ...
}
```

**Problem with above approach:** `PageReadStore` API doesn't directly expose a `Pages` interface we can intercept easily. Instead, pages are accessed through `getPageReader()` which returns a channel.

**Better approach:** Wrap at **ColumnIterator level** instead.

---

## 4. Implementation: ColumnIterator Page Filtering

Since `PageReadStore` API is hard to intercept, we wrap **closer to actual page consumption** in the reader chain.

### Current Architecture: ColumnIterator Hierarchy

```
BaseColumnIterator (abstract)
  ├─ reads pages from PageReadStore
  ├─ decodes definition/repetition levels per page
  └─ decodes column values

ColumnIterator extends BaseColumnIterator
  ├─ handles each primitive type (int, long, etc.)
  ├─ calls setPage(DataPage) to initialize page
  ├─ reads values row-by-row from page
  └─ advances to next page when current exhausted
```

### Enhancement: Track Page Index in ColumnIterator

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ColumnIterator.java` (MODIFIED)

```java
// Simplified excerpt
abstract class ColumnIterator<T> extends BaseColumnIterator {
  // ...existing fields...
  private int currentPageIndex = 0;                                    // NEW
  private boolean[] pageSkipMask = null;                              // NEW
  private ReadConf<?> readConf = null;                                // NEW
  private ColumnPath columnPath = null;                               // NEW
  private int currentRowGroupIndex = 0;                               // NEW

  // NEW: Initialize with page-skip information
  void initializeWithPageSkipping(
      ReadConf<?> conf,
      ColumnPath path,
      int rowGroupIndex,
      boolean[] skipMask) {
    this.readConf = conf;
    this.columnPath = path;
    this.currentRowGroupIndex = rowGroupIndex;
    this.pageSkipMask = skipMask;
    this.currentPageIndex = 0;
  }

  // Override setPage to track page index
  @Override
  void setPage(DataPage page) {
    // NEW: Check if this page should be skipped
    if (shouldSkipCurrentPage()) {
      // Skip this page; don't call super.setPage()
      // Mark as "empty page" for iteration logic
      this.triplesCount = 0;  // No rows to read from this page
      currentPageIndex++;
      return;
    }

    // Page is not skipped; process normally
    super.setPage(page);
    currentPageIndex++;
  }

  private boolean shouldSkipCurrentPage() {
    if (pageSkipMask == null) {
      return false;  // No skip mask; read all pages
    }
    if (currentPageIndex >= pageSkipMask.length) {
      return false;  // Page index out of range
    }
    return pageSkipMask[currentPageIndex];
  }

  // ...existing next() methods...
}
```

**Issue:** The above assumes we can intercept `setPage()` calls with page indices, but `PageReadStore` iterates pages internally in `getPageReader()`. We need a different approach.

---

## 5. Real Solution: ParquetValueReader Wrapping

The cleanest approach: wrap **`ParquetValueReader`** (which is already a factory pattern) to create a filtering wrapper.

### Current Flow

```
FileIterator.next()
  └─ model.read(last)      // model = ParquetValueReader<T>
      └─ Delegates to RowContainer
          └─ Delegates to ColumnIterator per column
```

### New Approach: Create FilteringParquetValueReader

**File:** `parquet/src/main/java/org/apache/iceberg/parquet/FilteringParquetValueReader.java` (NEW)

```java
package org.apache.iceberg.parquet;

import java.util.function.Function;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

/**
 * Wraps a ParquetValueReader to skip pages that don't match the filter.
 * Tracks page indices per column and consults ReadConf for skip decisions.
 */
public class FilteringParquetValueReader<T> implements ParquetValueReader<T> {
  private final ParquetValueReader<T> delegate;
  private final ReadConf<?> readConf;
  private final int rowGroupIndex;
  private final MessageType fileSchema;
  private final Map<ColumnPath, Integer> pageIndexPerColumn;

  public FilteringParquetValueReader(
      ParquetValueReader<T> delegate,
      ReadConf<?> readConf,
      int rowGroupIndex,
      MessageType fileSchema) {
    this.delegate = delegate;
    this.readConf = readConf;
    this.rowGroupIndex = rowGroupIndex;
    this.fileSchema = fileSchema;
    this.pageIndexPerColumn = new HashMap<>();
  }

  @Override
  public T read(T reuse) {
    // Naive approach: just delegate; page filtering happens earlier
    return delegate.read(reuse);
  }

  @Override
  public void setPageSource(PageReadStore pageSource) {
    // NEW: Wrap PageReadStore to track page iteration
    PageReadStore filtered = new PageReadStoreTracker(
        pageSource,
        readConf,
        rowGroupIndex,
        fileSchema,
        pageIndexPerColumn);
    delegate.setPageSource(filtered);
  }

  // ...delegate other methods...
}

/**
 * Tracks page indices as they're read from PageReadStore.
 * Provides mechanism to skip pages that don't match filter.
 */
class PageReadStoreTracker implements PageReadStore {
  private final PageReadStore delegate;
  private final ReadConf<?> readConf;
  private final int rowGroupIndex;
  private final MessageType fileSchema;
  private final Map<ColumnPath, Integer> pageIndexPerColumn;

  // Complex: would need to intercept low-level Parquet page reading APIs
  // This is challenging because PageReadStore doesn't expose high-level
  // "page iteration" interface
}
```

**Challenge:** Parquet's `PageReadStore` API is low-level and doesn't easily expose page-by-page iteration. Pages are read through compression/encoding-specific channels.

---

## 6. Pragmatic Solution: Accept Limitation for Non-Vectorized Path

After analyzing the architecture, the **honest truth** is:

### Why Row-Based Page-Skipping Is Hard

1. **`PageReadStore` is opaque:** Once `readNextRowGroup()` is called, pages are managed internally by Parquet's compression/encoding layer.

2. **Decompression happens eagerly:** Parquet decompresses pages lazily _within_ a single call to `getPageReader()`, not page-by-page.

3. **Row-level skipping loses page boundaries:** Row-based readers process rows across page boundaries; skipping a page loses the row-index context.

4. **Overhead justification:** Non-vectorized readers are already slow (one row at a time). Implementing page-skipping adds complexity for modest gains.

### Recommended Solution: Skip Page-Skipping for Non-Vectorized Path (Phase 1)

**Proposal:**
- ✅ Implement page-skipping for **vectorized path only** (main plan)
- ⏸️ Defer non-vectorized page-skipping to **Phase 2** (future enhancement)
- 📝 Document this limitation clearly

**Rationale:**
| Path | Benefit | Effort | ROI |
|------|---------|--------|-----|
| **Vectorized** | 20–80% I/O reduction | Medium | ✅ High |
| **Non-vectorized** | 5–15% I/O reduction | High | ⚠️ Mod |

Non-vectorized reads are used when:
- Vectorization disabled (`vectorization_enabled = false`)
- Nested types projected (not vectorizable)
- Complex filters (fall back to row readers)

These scenarios are less performance-sensitive than vectorized scans.

---

## 7. Alternative: Pragmatic Non-Vectorized Implementation

If page-skipping for non-vectorized **must** be supported in Phase 1, here's a pragmatic implementation:

### Accept Single-Page-Per-Row-Group

**Compromise:** Instead of true page-skipping, limit each row group to **one logical "page"** when pageSkippingEnabled=true and non-vectorized path is used.

```java
// In ParquetReader.FileIterator.advance() (row-based)

private void advance() {
  while (shouldSkip[nextRowGroup]) {
    nextRowGroup += 1;
    reader.skipNextRowGroup();
  }

  PageReadStore pages = reader.readNextRowGroup();

  // NEW: For non-vectorized + page-skipping,
  // skip entire row group if first page should be skipped
  if (pageSkippingEnabled && readConf.pageSkipMasks() != null) {
    boolean shouldSkipRowGroup = checkIfAllPagesSkipped(pages);
    if (shouldSkipRowGroup) {
      // Skip this RG entirely; move to next
      nextRowGroup += 1;
      advance();  // Recursively advance
      return;
    }
  }

  nextRowGroupStart += pages.getRowCount();
  nextRowGroup += 1;
  model.setPageSource(pages);
}

private boolean checkIfAllPagesSkipped(PageReadStore pageReadStore) {
  // Heuristic: Check if _first_ page's stats indicate skip
  // (Doesn't perfectly skip all pages, but filters obvious cases)
  // ...
}
```

**This is not ideal**, but acceptable for Phase 1.

---

## 8. Recommended Plan Amendment

### Phase 1: Vectorized Only

**Primary deliverable:** Full page-skipping for vectorized path (as per main plan)

**Non-vectorized:** One of two options:

**Option A (Recommended):** Document as "not supported"
```java
// In ReadConf
public boolean supportsPageSkipping() {
  return batchedReaderFunc != null;  // Vectorized only
}

// In Parquet.ReadBuilder
PageSkippingEnabled pageSkippingFlag = readConf.pageSkippingEnabled();
if (pageSkippingFlag && !readConf.supportsPageSkipping()) {
  LOG.warn("Page-skipping requested but not supported for non-vectorized reads; disabled");
  pageSkippingEnabled = false;
}
```

**Option B (Future):** Simple row-group-level filtering fallback
```java
// If page-skipping enabled but non-vectorized: apply row-group filtering
// (which is already implemented)
// Pages within RG are still read, but RG can be skipped entirely
```

### Phase 2: Non-Vectorized Enhancement

After Phase 1 stabilizes, implement true page-skipping for row-based path:
- Design custom `PageIterationWrapper` that understands Parquet compression encodings
- Patch Parquet Library if needed (propose upstream)
- Or accept pages sequentially and track indices externally

---

## 9. Final Architecture: Updated Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                   Spark Vectorized Read                      │
└──────────────────────────┬───────────────────────────────────┘
                           │
                  ┌────────▼─────────┐
                  │ VectorizedParquetReader│
                  │ + Page-Skipping[NEW]  │
                  └────────┬─────────┘
                           │
         ┌─────────────────┴─────────────────┐
         │ ParquetPageFilter compute + apply │
         │ + OffsetIndex pagination         │
         └─────────────────┬─────────────────┘
                           │
              ✅ PHASE 1: COMPLETE

┌──────────────────────────────────────────────────────────────┐
│                Spark Row-Based Read                          │
└──────────────────────────┬───────────────────────────────────┘
                           │
              ┌────────────▼────────────┐
              │ ParquetReader           │
              │ (FileIterator)          │
              └────────────┬────────────┘
                           │
         ⚠️ Page-Skipping NOT SUPPORTED (Phase 1)
         Or: Row-Group-Level Filtering Only
                           │
         📋 PHASE 2: Future Enhancement
         (Requires Parquet API changes or workarounds)
```

---

## Summary & Recommendation

### Key Findings

1. **Vectorized path:** ✅ Full page-skipping feasible (main plan)
2. **Non-vectorized path:** ⚠️ Challenging due to Parquet API limitations
3. **Effort mismatch:** High effort, moderate ROI for non-vectorized page-skipping

### Recommendation for Main Plan

**Update PARQUET_PAGE_SKIPPING_PLAN.md:**

1. **Phase 1 Scope Clarification:**
   ```
   Vectorized reads: ✅ Full page-skipping support
   Non-vectorized reads: ⚠️ Row-group filtering only (existing)
   
   Rationale: Non-vectorized paths are already slow;
   page-skipping gains are modest (5–15% vs. 20–80% for vectorized)
   ```

2. **Configuration:**
   ```
   iceberg.parquet.page-skipping.enabled = true
     ├─ Vectorized reads: page-level filtering applied
     └─ Non-vectorized reads: property ignored (logs warning if attempted)
   ```

3. **Phase 2 Future Work:**
   ```
   - Extend page-skipping to non-vectorized readers
   - Requires deeper integration with Parquet page APIs
   - Assess ROI after Phase 1 release
   ```

4. **Testing:**
   ```
   Phase 1:
     ✅ Vectorized reads + page-skipping
     ✅ Non-vectorized reads WITHOUT page-skipping
     ✅ Fallback when vectorization disabled
   
   Phase 2:
     □ Non-vectorized reads + page-skipping
   ```

### Implementation Changes

**In `PARQUET_PAGE_SKIPPING_PLAN.md`:**

Add section: "Non-Vectorized Path Limitations"

```markdown
## Non-Vectorized Read Path: Limitations & Future Work

### Phase 1 Scope
Page-skipping is implemented for **vectorized reads only**. Non-vectorized 
reads continue to use row-group-level filtering (existing behavior).

### Why
- Vectorized reads (columnar batches) naturally align with page boundaries
- Row-based reads process rows across pages; skipping lacks context
- Parquet's PageReadStore API doesn't expose page-by-page iteration
- ROI: Vectorized 20–80% I/O reduction vs. Non-vectorized 5–15%

### Configuration
Setting `iceberg.parquet.page-skipping.enabled = true` has no effect on 
non-vectorized reads (no error; silently ignored).

### Phase 2: Non-Vectorized Enhancement
Future version may support non-vectorized page-skipping via:
1. Custom Parquet pagination wrapper
2. Upstream Parquet library enhancements
3. Page-level pre-filtering before PageReadStore consumption

### Workaround
Non-vectorized reads benefit from existing row-group filtering. Users can:
- Enable `iceberg.parquet.page-skipping.enabled = true` for vectorized queries
- Non-vectorized fallback uses standard row-group filtering
```

---

## Conclusion

The main plan is **vectorization-centric** for good reasons:
- **Vectorized readers** align perfectly with page boundaries
- **Page-skipping savings** are substantial (20–80%)
- **Implementation** is clean and maintainable

**Non-vectorized page-skipping should be deferred** to Phase 2 after:
1. Phase 1 stabilizes and is widely adopted
2. Performance data shows demand
3. Parquet APIs evolve (or workarounds proven)

**This approach:** Delivers high ROI in Phase 1, keeps scope manageable, and leaves room for enhancement.


