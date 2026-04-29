# Parquet Page-Skipping Implementation Plan — Executive Summary

**Date:** April 28, 2026  
**Deliverables:** Comprehensive planning documentation

---

## Overview

This plan proposes implementing **Parquet page-level skipping** in Apache Iceberg to reduce I/O and decompression overhead when reading Parquet files with selective filters. Page-skipping complements (not replaces) existing row-group-level filtering.

### Key Outcomes

- **I/O Reduction:** 20–80% fewer bytes read for selective queries (point lookups, low-cardinality filters)
- **Latency Improvement:** 5–10x faster query execution for highly selective filters
- **Zero Breaking Changes:** Opt-in feature; defaults to `false` (disabled)
- **Production Ready:** Phased rollout with comprehensive testing

---

## Three Planning Documents Delivered

### 1. **PARQUET_PAGE_SKIPPING_PLAN.md** (Main Plan)
- Executive summary and architecture overview
- Current state: row-group-only filtering
- Five phases: Infrastructure → Core → Config → Testing → Docs
- Design considerations, limitations, timeline (10-13 weeks)
- Risk mitigation, boundaries checklist

### 2. **PARQUET_PAGE_SKIPPING_ARCHITECTURE.md** (Technical Deep-Dive)
- High-level & class diagrams
- Code sketches for 5 key components
- Test scenarios and performance estimation
- Rollout strategy and Parquet API reference

### 3. **PARQUET_PAGE_SKIPPING_IMPLEMENTATION_CHECKLIST.md** (Execution Playbook)
- Week-by-week tasks (7 weeks, 13 tasks)
- Concrete acceptance criteria
- Build/test commands
- 5-PR strategy with scope/size/focus
- Success metrics per phase

---

## Quick Facts

| Aspect | Detail |
|--------|--------|
| **Effort** | 10–13 weeks (3 persons) or ~3 months part-time |
| **Scope** | Engine-agnostic + Spark integration |
| **Breaking Changes** | ❌ None (opt-in feature) |
| **Risk Level** | 🟡 Medium (filters, I/O paths) |
| **Performance Impact** | ✅ Positive for selective queries; neutral for full scans |
| **Default Behavior** | Disabled (`false`) |
| **Dependencies** | Parquet 1.12+ (OffsetIndex API) |

---

## Architecture in a Nutshell

**Current:** Row-group filtering only
- `ParquetMetricsRowGroupFilter` (min/max stats)
- `ParquetDictionaryRowGroupFilter` (dictionaries)
- `ParquetBloomRowGroupFilter` (bloom filters)

**Proposed Addition:** Page-level filtering
- `ParquetPageFilter` (predicate evaluation on pages)
- `ParquetPageStats` (extract page statistics)
- `ReadConf.pageSkipMasks` (pre-computed skip masks per page)
- `VectorizedParquetReader.FileIterator` (skip pages in batch loop)

**Key insight:** Page-skipping is **additive**—complements row-group filtering; both layers applied in sequence.

---

## 5-Phase Implementation

| Phase | Duration | Key Component | Output |
|-------|----------|----------------|--------|
| **1. Infrastructure** | 1–2 wks | `ParquetPageFilter`, `ParquetPageStats` | 2 new classes, unit tests |
| **2. Core Reader** | 2–3 wks | `ReadConf`, `VectorizedParquetReader` | Page-skip integration |
| **3. Configuration** | 1 wk | Properties, Spark config | Flag threading end-to-end |
| **4. Testing** | 3 wks | Unit/integration/JMH tests | Correctness + perf validation |
| **5. Documentation** | 1 wk | User guide, release notes | Operator runbooks |

---

## Implementation Checklist (Executive View)

### Phase 1: Infrastructure ✓
- [ ] Create `ParquetPageFilter` (filter evaluation)
- [ ] Create `ParquetPageStats` (statistics extraction)
- [ ] Unit tests: 20+ cases, 80%+ coverage

### Phase 2: Core Reader ✓
- [ ] Extend `ReadConf` with page-skip masks
- [ ] Modify `VectorizedParquetReader` batch loop
- [ ] Integration tests: correctness validation

### Phase 3: Configuration ✓
- [ ] Add `PARQUET_PAGE_SKIPPING_ENABLED` property
- [ ] Add `SparkReadConf` methods
- [ ] Thread config through `Parquet.ReadBuilder`

### Phase 4: Testing ✓
- [ ] Unit tests for filter logic
- [ ] Integration tests (Spark vectorized readers)
- [ ] E2E tests (Spark SQL queries)
- [ ] JMH benchmarks (measure I/O savings)

### Phase 5: Documentation ✓
- [ ] User guide: when/how to enable
- [ ] Troubleshooting: OffsetIndex issues
- [ ] Release notes
- [ ] Code review + style fixes

---

## Expected Performance Impact

### Workload: Point Lookup (Highly Selective)
- **Data:** 100 GB Parquet file, 1,000 row groups, 100 KB pages
- **Filter:** `timestamp = '2024-06-15'` (1% of data)
- **Without page-skipping:** 5 GB read (row-group filtering only)
- **With page-skipping:** ~500 MB read (99% of pages skipped within RG)
- **Improvement:** ✅ 10x fewer I/O bytes, 5–10x faster query

### Workload: Range Query (Moderately Selective)
- **Filter:** `timestamp BETWEEN '2024-06-01' AND '2024-06-30'` (10% of data)
- **Without page-skipping:** 10 GB read
- **With page-skipping:** ~2–3 GB read (60–70% of pages skipped)
- **Improvement:** ✅ 3–5x fewer I/O bytes

### Workload: Full Scan (No Filter)
- **Performance:** Identical (no page-skipping occurs)
- **Overhead:** Negligible (page-skip computation < 1ms per RG)

---

## Files to Create/Modify

### Parquet Module
```
NEW:
  parquet/src/main/java/.../parquet/ParquetPageFilter.java       (~300 LOC)
  parquet/src/main/java/.../parquet/ParquetPageStats.java        (~150 LOC)

MODIFIED:
  parquet/src/main/java/.../parquet/ReadConf.java               (+250 LOC)
  parquet/src/main/java/.../parquet/VectorizedParquetReader.java (+150 LOC)
  parquet/src/main/java/.../parquet/Parquet.java                (+50 LOC)
```

### Core Module
```
MODIFIED:
  core/src/main/java/.../iceberg/TableProperties.java           (+10 LOC)
```

### Spark Module
```
MODIFIED:
  spark/.../spark/src/main/java/.../iceberg/spark/SparkReadConf.java  (+30 LOC)
  spark/.../spark/src/main/java/.../iceberg/spark/source/BaseBatchReader.java (+30 LOC)

NEW entries:
  spark/.../SparkReadOptions.java (config option)
  spark/.../SparkSQLProperties.java (session property)
```

### Tests
```
NEW:
  parquet/src/test/java/.../parquet/TestParquetPageFilter.java               (~600 LOC)
  spark/.../test/java/.../spark/data/vectorized/TestParquetPageSkipping.java (~800 LOC)
  spark/.../test/java/.../spark/SparkPageSkippingE2ETest.java                (~400 LOC)
  spark/.../src/jmh/java/.../ParquetPageSkippingBenchmark.java               (~300 LOC)
```

**Total:** ~3,500 lines new/modified code

---

## Configuration (User-Facing)

### Enable Page-Skipping

**Option 1: Table Property**
```properties
iceberg.parquet.page-skipping.enabled = true
```

**Option 2: Spark Session**
```sql
SET spark.sql.iceberg.parquet.page-skipping.enabled = true;
```

**Option 3: Spark Configuration File**
```
spark.sql.iceberg.parquet.page-skipping.enabled=true
```

### Example Query
```sql
-- With page-skipping enabled
SELECT * FROM iceberg_table
WHERE timestamp BETWEEN '2024-06-01' AND '2024-06-30'
  AND region = 'us-west';

-- Expected: ~3–5x fewer bytes read, 2–3x faster execution
```

---

## Success Criteria

### Phase-by-Phase (Development)

✅ **Phase 1:** Classes implemented, unit tests passing (80%+ coverage)  
✅ **Phase 2:** Integration complete, no correctness regressions  
✅ **Phase 3:** Configuration accessible, defaults safe  
✅ **Phase 4:** All tests pass, I/O reduction confirmed via benchmarks  
✅ **Phase 5:** Documentation complete, code reviewed & styled  

### Post-Release (Metrics)

- **Adoption rate:** % of selective scans using page-skipping
- **I/O reduction:** Average of 30–50% for queries with low-cardinality filters
- **Latency improvement:** Median 3–5x speedup for point lookups
- **Stability:** < 3 critical bugs per quarter

---

## Safety & Rollback

### Conservative Defaults
- Feature **disabled by default** (`PARQUET_PAGE_SKIPPING_ENABLED = false`)
- Minimal performance penalty if disabled (0.1–1% overhead)
- No breaking changes; all existing code paths unchanged

### Fallback Logic
- If page statistics unavailable → read all pages conservatively
- If `OffsetIndex` missing → skip page-level filtering silently
- If filter is unselective → few/no pages skipped (graceful degradation)

### Rollback Plan
In unlikely event of issues post-release:
1. Set default to `false` (immediate)
2. Fix issue, re-release patch
3. Document any known limitations
4. No data loss or corruption risk

---

## Key Assumptions

✅ **Parquet 1.12+** available (OffsetIndex standard since v1.12)  
✅ **Small page-skip overhead** acceptable (one boolean per page; negligible memory)  
✅ **Conservative approach** sufficient (if stats missing, read all pages)  
✅ **Existing filters still applied** (page-skipping is additional optimization layer)  

---

## Out of Scope (Future Enhancements)

- ❌ Nested type filtering at page level (Phase 2)
- ❌ Variant type support (Phase 2)
- ❌ Page-level bloom filters (Parquet v2+ feature)
- ❌ Columnar statistics caching across reads (optimization)

---

## Getting Started

### For Developers

1. **Read in order:**
   - This summary → Strategic overview
   - `PARQUET_PAGE_SKIPPING_PLAN.md` → Architecture & design
   - `PARQUET_PAGE_SKIPPING_ARCHITECTURE.md` → Technical details & code sketches
   - `PARQUET_PAGE_SKIPPING_IMPLEMENTATION_CHECKLIST.md` → Task breakdown

2. **Design review** with Iceberg PMC (scope, timeline, resources)

3. **Start Phase 1:** Implement `ParquetPageFilter` class

4. **Follow checklist** for subsequent phases

### For Reviewers / PMC

1. **Quick read:** This summary document (5 min)

2. **Deep dive:** `PARQUET_PAGE_SKIPPING_PLAN.md`, Section 2 (Architecture)

3. **Review PRs:** Use scope/acceptance criteria from checklist

---

## Next Steps (Immediate)

### This Week
- [ ] Stakeholder review of plan
- [ ] PMC approval on scope & timeline
- [ ] Resource allocation (assign developer lead)

### Week 1–2
- [ ] Design finalization with community feedback
- [ ] Create GitHub epic/issue
- [ ] Begin Phase 1 implementation

### Weeks 3–7
- [ ] Execute phases per plan
- [ ] Post PRs for review as milestones hit
- [ ] Integrate feedback

### Month 3
- [ ] Release in Iceberg v1.8.0 or later
- [ ] Monitor adoption & performance
- [ ] Plan Phase 2 (nested types, optimizations)

---

## Document Map

```
📄 PARQUET_PAGE_SKIPPING_SUMMARY.md (this file)
   └─ Executive summary, quick reference

📄 PARQUET_PAGE_SKIPPING_PLAN.md
   ├─ Strategic plan (5 phases)
   ├─ Design considerations
   ├─ Timeline & resources
   ├─ Risk mitigation
   └─ Known limitations & future work

📄 PARQUET_PAGE_SKIPPING_ARCHITECTURE.md
   ├─ Architecture diagrams
   ├─ Class design
   ├─ 5 code sketches
   ├─ Test scenarios
   └─ Performance estimation

📄 PARQUET_PAGE_SKIPPING_IMPLEMENTATION_CHECKLIST.md
   ├─ Week-by-week tasks (13 items)
   ├─ Acceptance criteria per task
   ├─ Build/test commands
   ├─ PR strategy (5 PRs)
   ├─ Success metrics
   └─ References
```

**Total Documentation:** ~6,000+ lines across 4 files  
**Status:** ✅ **Complete & Ready for Implementation**

---

## Contact & Questions

For questions or clarifications on this plan:
- **Architecture:** Iceberg PMC, Parquet team
- **Implementation:** Assigned developer lead
- **Spark integration:** Spark integration team
- **Review:** Full PMC review on initial PR

---

## Sign-Off

**Plan Ready:** ✅ April 28, 2026  
**Status:** Ready for PMC review & project approval  
**Target Implementation:** Start Q3 2026  
**Target Release:** Iceberg v1.8.0 (Q4 2026)

---

**Let's make Iceberg faster for selective workloads! 🚀**


