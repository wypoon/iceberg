# Parquet Page-Skipping: Next Steps & Implementation Checklist

**Companion to:** `PARQUET_PAGE_SKIPPING_PLAN.md` and `PARQUET_PAGE_SKIPPING_ARCHITECTURE.md`  
**Date:** April 28, 2026  
**Status:** Ready for Development

---

## 1. Quick Start: Development Path

### Week 1: Infrastructure & Prototyping

#### Task 1.1: Create ParquetPageFilter Class
**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageFilter.java`

**Checklist:**
- [ ] Copy structure from `ParquetMetricsRowGroupFilter`
- [ ] Implement `BoundExpressionVisitor` for page evaluation
- [ ] Add methods: `shouldRead()`, `filterPages()`
- [ ] Handle fallback: missing stats → conservative "might match"
- [ ] Add unit tests: `TestParquetPageFilter` (basic cases)

**Estimated effort:** 3–4 days

**Acceptance criteria:**
```
✓ Can evaluate simple predicates (lt, gt, eq, in) on page stats
✓ Returns correct boolean[] skip masks
✓ Handles null/missing stats gracefully
✓ 80%+ coverage in unit tests
```

---

#### Task 1.2: Create ParquetPageStats Utility
**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageStats.java`

**Checklist:**
- [ ] Implement `getPageStats(columnChunk, pageIndex, offsetIndex): Statistics`
- [ ] Implement `getPageRowCount(pageLocation): long`
- [ ] Implement `hasValidStats(pageLocation): boolean`
- [ ] Add null checks and graceful fallbacks
- [ ] Document Parquet API compatibility requirements

**Estimated effort:** 1–2 days

**Dependencies:**
- Requires Parquet 1.12+ (OffsetIndex API)
- Check `gradle/libs.versions.toml` for current version

---

### Week 2: Core Reader Integration

#### Task 2.1: Extend ReadConf with Page-Skip Masks
**File:** `parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java`

**Changes:**
```
1. Add field: Map<String, boolean[]> pageSkipMasks
2. Add field: boolean pageSkippingEnabled (constructor param)
3. Add method: computePageSkipMasks(fileSchema, filter, caseSensitive)
4. Add method: shouldSkipPage(rgIdx, colPath, pageIdx): boolean
5. Integrate into ReadConf constructor logic
```

**Checklist:**
- [ ] Add state fields with proper initialization
- [ ] Implement page-skip computation (loop over RGs and columns)
- [ ] Add thread-safe concurrent access (if needed)
- [ ] Write unit tests for `shouldSkipPage()` logic
- [ ] Verify no performance regression in page computation

**Estimated effort:** 3–4 days

**Test scenarios:**
```
✓ Page masks computed only for non-row-group-skipped data
✓ shouldSkipPage() returns false if no mask (conservative)
✓ Handles missing OffsetIndex gracefully
✓ Multiple filters (AND/OR) combine correctly
```

---

#### Task 2.2: Modify VectorizedParquetReader Page Loop
**File:** `parquet/src/main/java/org/apache/iceberg/parquet/VectorizedParquetReader.java`

**Changes:**
```
1. Add constructor param: boolean pageSkippingEnabled
2. Modify FileIterator.next() to skip pages
3. Track currentPageIndex and currentColumnPath
4. Call readConf.shouldSkipPage() before reading
5. Handle end-of-batch when pages are skipped
```

**Checklist:**
- [ ] Add state tracking for page iteration
- [ ] Implement skip logic (loop until non-skipped page found)
- [ ] Handle edge case: all pages in RG are skipped
- [ ] Test with existing vectorized readers (Spark, Arrow)
- [ ] Verify no corruption of batches when pages are skipped

**Estimated effort:** 3–4 days

**Test scenarios:**
```
✓ Pages skipped; batches contain only non-skipped page data
✓ Batch sizes correct (may be < configured batch size)
✓ Works with reused containers (reuseContainers=true)
✓ Vectorized batch counts match expected row counts
```

---

### Week 3: Configuration & Spark Integration

#### Task 3.1: Add Table Property & Config Classes
**Files:**
- `core/src/main/java/org/apache/iceberg/TableProperties.java`
- `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java`

**Checklist:**
- [ ] Add `PARQUET_PAGE_SKIPPING_ENABLED` property to `TableProperties`
- [ ] Add `PARQUET_PAGE_SKIPPING_MIN_BYTES` property (optional)
- [ ] Add methods to `SparkReadConf` (with config precedence: option → session → table)
- [ ] Add `SparkReadOptions` entries
- [ ] Add `SparkSQLProperties` entries

**Estimated effort:** 1 day

**Properties:**
```java
PARQUET_PAGE_SKIPPING_ENABLED           // bool, default=false
PARQUET_PAGE_SKIPPING_MIN_BYTES         // long, default=10KB
PARQUET_PAGE_SKIPPING_FALLBACK_WITHOUT_INDEX // bool, default=true
```

---

#### Task 3.2: Thread Configuration Through Parquet.ReadBuilder
**File:** `parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java`

**Changes:**
```
1. Add field to ReadBuilder: boolean pageSkippingEnabled = false
2. Add method: ReadBuilder.pageSkippingEnabled(bool)
3. Pass flag to ReadConf constructor
4. Pass flag to VectorizedParquetReader constructor
```

**Checklist:**
- [ ] ReadBuilder accepts flag
- [ ] Flag correctly threaded to ReadConf
- [ ] Flag correctly threaded to VectorizedParquetReader
- [ ] Verify behavior when flag=false (all pages read)

**Estimated effort:** 1 day

---

#### Task 3.3: Spark Batch Reader Integration
**File:** `spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/BaseBatchReader.java`

**Changes:**
```
1. Read pageSkippingEnabled from SparkReadConf
2. Pass to FormatModelRegistry.readBuilder(...) or Parquet.read()
3. Verify integration with delete filters
```

**Checklist:**
- [ ] Config read correctly from SparkReadConf
- [ ] Flag passed through reader factory chain
- [ ] Works with both SparkColumnarReaderFactory and SparkRowReaderFactory paths
- [ ] Integration test: end-to-end Spark query with page-skipping

**Estimated effort:** 1–2 days

---

### Week 4–6: Testing & Validation

#### Task 4.1: Unit Tests for ParquetPageFilter
**File:** `parquet/src/test/java/org/apache/iceberg/parquet/TestParquetPageFilter.java`

**Test matrix:**
```
Predicates:
  ✓ lt, lte, gt, gte, eq, neq, in
  ✓ AND, OR, NOT combinations
  ✓ Complex nested predicates

Statistics scenarios:
  ✓ Page stats available
  ✓ Page stats missing (fallback)
  ✓ Null stats (empty pages)
  ✓ All-null pages vs. no-nulls pages

Column types:
  ✓ INTEGER, LONG, FLOAT, DOUBLE
  ✓ BINARY, FIXED_LEN_BYTE_ARRAY
  ✓ BOOLEAN
```

**Checklist:**
- [ ] 20+ test cases covering predicate logic
- [ ] 80%+ code coverage
- [ ] All test cases pass
- [ ] Performance: filter evaluation < 1ms per page

**Estimated effort:** 3–4 days

---

#### Task 4.2: Integration Tests: Vectorized Reader + Page-Skipping
**File:** `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/data/vectorized/parquet/TestParquetPageSkipping.java`

**Test scenarios:**
```
Feature tests:
  ✓ Page-skipping enabled/disabled
  ✓ Vectorized batches with skipped pages
  ✓ Correctness: result matches non-skipped read
  ✓ Multi-column projection
  ✓ Delete file interaction

Edge cases:
  ✓ Empty row groups
  ✓ Single-page row groups
  ✓ Files without OffsetIndex
  ✓ All pages skipped in RG (shouldn't happen)
  ✓ Encrypted files
  ✓ Compressed pages

Performance:
  ✓ I/O bytes reduced as expected
  ✓ Query latency improved for selective filters
```

**Checklist:**
- [ ] Create test data (various configurations)
- [ ] 15+ test cases
- [ ] Correctness validation (results match expected)
- [ ] Performance baseline captured

**Estimated effort:** 3–4 days

---

#### Task 4.3: Spark SQL End-to-End Tests
**File:** `spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/SparkPageSkippingE2ETest.java`

**Test scenarios:**
```
SQL queries:
  ✓ SELECT * WHERE timestamp > X       (point range)
  ✓ SELECT * WHERE id IN (1, 2, 3)    (low cardinality)
  ✓ SELECT col WHERE flag = true       (boolean)
  ✓ Joins with page-skipping enabled
  ✓ Aggregations with WHERE clause

Configuration:
  ✓ Table property override
  ✓ Session config override
  ✓ Read option override
```

**Checklist:**
- [ ] 10+ SQL queries tested
- [ ] Results correct (compare with baseline)
- [ ] Performance captured (I/O reduction %)
- [ ] No regressions

**Estimated effort:** 2–3 days

---

#### Task 4.4: JMH Benchmarks
**File:** `spark/v4.1/spark/src/jmh/java/org/apache/iceberg/spark/data/parquet/PageSkippingBenchmark.java`

**Benchmarks:**
```
Workloads:
  ✓ Point lookups (high selectivity)
  ✓ Range queries (medium selectivity)
  ✓ Full table scans (low selectivity)
  ✓ Multi-predicate filters

Metrics tracked:
  ✓ Throughput (rows/sec)
  ✓ Latency (p50, p95, p99)
  ✓ I/O bytes (via perf or JFR)
  ✓ CPU time
```

**Checklist:**
- [ ] 3–4 benchmark scenarios
- [ ] Run with page-skipping on/off
- [ ] Results published to team
- [ ] Identify performance cliff cases

**Estimated effort:** 2–3 days

---

### Week 7: Documentation & Polish

#### Task 5.1: Code Comments & Documentation
**Checklist:**
- [ ] All public methods have comprehensive Javadoc
- [ ] Explain page-skip logic in comments
- [ ] Document assumptions (e.g., OffsetIndex availability)
- [ ] Add examples for common use cases

**Estimated effort:** 1 day

---

#### Task 5.2: User & Operator Documentation
**Files:**
- `docs/docs/spark-configuration.md`
- `docs/docs/performance-tuning.md`
- Release notes

**Content:**
```
1. Feature overview: "What is page-skipping?"
2. When to enable: "Selective queries, low-cardinality filters"
3. Configuration: Properties and Spark SQL config
4. Performance impact: "Typical I/O reduction: 20–80%"
5. Troubleshooting: "Page-skipping not working; why?" (OffsetIndex issue)
6. FAQ
```

**Checklist:**
- [ ] User guide written
- [ ] Examples provided
- [ ] Troubleshooting guide
- [ ] Release notes updated

**Estimated effort:** 1–2 days

---

#### Task 5.3: Code Review & Cleanup
**Checklist:**
- [ ] All code follows Apache Iceberg style guide
- [ ] Run spotless formatter: `./gradlew spotlessApply`
- [ ] Verify no new warnings or lint issues
- [ ] Check for TODOs/FIXMEs (resolve or document)
- [ ] Prepare PR(s) for review

**Estimated effort:** 1 day

---

## 2. Detailed Implementation Checklist

### Core Files to Modify/Create

```
CORE (engine-agnostic):
  ✓ parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageFilter.java       [NEW]
  ✓ parquet/src/main/java/org/apache/iceberg/parquet/ParquetPageStats.java        [NEW]
  ✓ parquet/src/main/java/org/apache/iceberg/parquet/ReadConf.java                [MODIFIED]
  ✓ parquet/src/main/java/org/apache/iceberg/parquet/VectorizedParquetReader.java [MODIFIED]
  ✓ parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java                 [MODIFIED]
  ✓ core/src/main/java/org/apache/iceberg/TableProperties.java                    [MODIFIED]

SPARK:
  ✓ spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkReadConf.java          [MODIFIED]
  ✓ spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkReadOptions.java       [NEW entries]
  ✓ spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/SparkSQLProperties.java     [NEW entries]
  ✓ spark/v4.1/spark/src/main/java/org/apache/iceberg/spark/source/BaseBatchReader.java [MODIFIED]

TESTS:
  ✓ parquet/src/test/java/org/apache/iceberg/parquet/TestParquetPageFilter.java                [NEW]
  ✓ spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/data/TestReadConf.java            [MODIFIED]
  ✓ spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/data/vectorized/parquet/TestParquetPageSkipping.java [NEW]
  ✓ spark/v4.1/spark/src/test/java/org/apache/iceberg/spark/SparkPageSkippingE2ETest.java      [NEW]

BENCHMARKS:
  ✓ spark/v4.1/spark/src/jmh/java/org/apache/iceberg/spark/data/parquet/PageSkippingBenchmark.java [NEW]

DOCS:
  ✓ docs/docs/spark-configuration.md                                              [MODIFIED]
  ✓ docs/docs/performance-tuning.md                                                [MODIFIED]
```

---

## 3. Build & Test Commands

### Local Development

```bash
# Build without tests
./gradlew build -x test -x integrationTest

# Run parquet module tests
./gradlew :iceberg-parquet:test

# Run specific test
./gradlew :iceberg-parquet:test \
  --tests "org.apache.iceberg.parquet.TestParquetPageFilter"

# Run Spark page-skipping tests
./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:test \
  --tests "org.apache.iceberg.spark.data.vectorized.parquet.TestParquetPageSkipping"

# Code formatting
./gradlew spotlessApply

# Check formatting
./gradlew spotlessCheck

# API compatibility check
./gradlew revApiCheck

# Run JMH benchmarks
./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh \
  -Pjmh.benchmarks="PageSkippingBenchmark"
```

---

## 4. PR Strategy

### PR 1: Infrastructure (ParquetPageFilter + ParquetPageStats)
```
Title: "Parquet: Add page-level filter evaluation (infrastructure)"
Scope: New classes + basic unit tests
Size: ~500 lines
Review focus: Correctness of filter logic, Parquet API usage
```

### PR 2: ReadConf + VectorizedParquetReader Integration
```
Title: "Parquet: Integrate page-skipping into ReadConf and VectorizedParquetReader"
Scope: Core reader modifications
Size: ~800 lines
Review focus: Performance impact, thread safety, correctness
```

### PR 3: Configuration & Spark Integration
```
Title: "Spark: Add page-skipping configuration and batch reader integration"
Scope: Table properties, Spark config, batch reader hookup
Size: ~400 lines
Review focus: Config precedence, backward compatibility
```

### PR 4: Tests & Benchmarks
```
Title: "Tests: Add comprehensive tests and benchmarks for page-skipping"
Scope: Unit tests, integration tests, JMH benchmarks
Size: ~2000 lines
Review focus: Test coverage, correctness validation
```

### PR 5: Documentation
```
Title: "Docs: Page-skipping user guide and release notes"
Scope: User documentation, troubleshooting, examples
Size: ~400 lines
Review focus: Clarity, accuracy, completeness
```

---

## 5. Risk & Rollback Plan

### Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Incorrect page stats → missing rows | 🔴 Critical | Conservative fallback; extensive unit tests |
| Performance regression (RG-only files) | 🟡 Medium | Benchmarks; default off; toggle in code |
| Parquet compatibility (old files) | 🟡 Medium | Graceful fallback; test with Parquet 1.10+ |
| Memory overhead (page masks) | 🟢 Low | One bool/page; negligible for typical files |
| Interaction with encryption | 🟡 Medium | Test with encrypted files; document limitation |

### Rollback Plan

If issues found  **post-release**:

1. **Immediate:** Set `PARQUET_PAGE_SKIPPING_ENABLED_DEFAULT = false`
2. **Short-term:** Fix issue, re-release patch
3. **Document:** Known issues page on Iceberg site

**No breaking changes:** Feature is opt-in; old code paths untouched.

---

## 6. Success Criteria

### Phase-by-Phase

#### Phase 1 ✓
- [ ] ParquetPageFilter + ParquetPageStats classes implemented
- [ ] Unit tests pass (20+ cases, 80%+ coverage)
- [ ] Code review approved

#### Phase 2 ✓
- [ ] ReadConf tracks page-skip masks
- [ ] VectorizedParquetReader skips pages
- [ ] No performance regression
- [ ] Integration tests pass

#### Phase 3 ✓
- [ ] Table properties defined and accessible
- [ ] Spark config plumbing complete
- [ ] Configuration tests pass

#### Phase 4 ✓
- [ ] All unit + integration tests pass
- [ ] Benchmark shows I/O reduction for selective queries
- [ ] No correctness issues (results = baseline)
- [ ] Edge cases handled

#### Phase 5 ✓
- [ ] User documentation complete
- [ ] Code conforms to project style
- [ ] All PRs reviewed and approved
- [ ] Release notes published

### Post-Release Metrics

Track in Iceberg telemetry/monitoring:

```
✓ Feature adoption rate (% of scans using page-skipping)
✓ I/O reduction achieved (bytes saved)
✓ Query latency improvement (p50, p95)
✓ Bug/issue reports (track and fix)
```

---

## 7. References & Resources

### Documentation
- [Apache Parquet Spec: Page Index](https://github.com/apache/parquet-format/blob/master/PageIndex.md)
- [Iceberg Parquet Reader](https://iceberg.apache.org/)
- [Spark Vectorization](https://docs.databricks.com/en/sql/language-manual/index.html)

### Key Classes (review before coding)
- `org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter` (model/reference)
- `org.apache.parquet.hadoop.metadata.OffsetIndex` (Parquet API)
- `org.apache.parquet.hadoop.ParquetFileReader` (file I/O)
- `org.apache.iceberg.parquet.VectorizedParquetReader` (vectorization)

### Slack/Chat Channels
- `#iceberg-dev`: Ask questions, get unblocked
- `#iceberg-spark`: Spark-specific discussions
- `#iceberg-parquet`: Parquet format discussions

---

## 8. Sign-Off

**Plan Owner:** [Your Name / Team]  
**Stakeholders:** Iceberg PMC, Spark Integration Team  
**Last Updated:** April 28, 2026  
**Status:** ✅ **Ready for Implementation**

---

## Appendix: Example: Enabling Page-Skipping in Spark SQL

```sql
-- Session-level configuration
SET spark.sql.iceberg.parquet.page-skipping.enabled=true;

-- Query with selective filter
SELECT id, timestamp, value
FROM iceberg_table
WHERE timestamp > '2024-06-01'
  AND timestamp < '2024-06-30'
  AND region = 'us-west';
  
-- Expected result:
-- ✓ Row-group filtering: eliminates RGs with all dates outside range
-- ✓ Page filtering: eliminates pages within matching RGs with all dates outside range
-- ✓ I/O reduction: 60–80% fewer bytes read
-- ✓ Latency improvement: 5–10x faster (typical for selective queries)
```

---

## Version History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 1.0 | 2026-04-28 | Planning Team | Initial comprehensive plan |


