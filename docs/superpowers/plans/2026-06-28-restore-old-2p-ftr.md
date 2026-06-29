# Restore Old 2P FTR Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the full no-MBR 2P search and report the old MBR-vs-noMBR FTR metrics alongside the current split-search FTR metrics.

**Architecture:** Keep `search_combined` as the full MBR search and add `search_noMBR` as the restored full no-MBR search. Extend FTR metric computation so a configured `no_mbr_search` produces a nested `mbr_vs_no_mbr` metrics block, while the existing split-search calculation keeps its current top-level metric names for report continuity.

**Tech Stack:** Julia metrics code using Arrow/DataFrames/JSON, search parameter JSON, and the existing HTML metrics report flattener.

---

### Task 1: FTR Metric Tests

**Files:**
- Modify: `julia/ftr_metrics_test.jl`

- [ ] **Step 1: Write the failing test**

Add no-MBR fixture tables and assert that `compute_ftr_metrics` returns both existing top-level FTR metrics and an `mbr_vs_no_mbr` FTR block. The top-level metrics must keep the split-search denominator; the `mbr_vs_no_mbr` block must compare `search_combined` with `search_noMBR` only across human-only runs.

- [ ] **Step 2: Run test to verify it fails**

Run: `julia --project=julia julia/ftr_metrics_test.jl`
Expected: FAIL because the returned metrics do not include the `mbr_vs_no_mbr` block.

### Task 2: FTR Metric Implementation

**Files:**
- Modify: `julia/metrics/ftr_metrics.jl`

- [ ] **Step 1: Implement minimal metric code**

Keep existing split-search metrics at their top-level names. Add helpers that compare combined MBR tables with configured no-MBR tables and return the same `additional_yeast_IDs`, `additional_IDs`, and `false_transfer_rate` keys under `mbr_vs_no_mbr`.

- [ ] **Step 2: Run FTR tests**

Run: `julia --project=julia julia/ftr_metrics_test.jl`
Expected: PASS.

### Task 3: Params and Report

**Files:**
- Add: `params/EWZ_2P_MBR_Exploris/search_noMBR.json`
- Add: `params/EWZ_2P_MBR_Exploris/resources_noMBR.json`
- Modify: `params/EWZ_2P_MBR_Exploris/metrics.json`
- Modify: `params/EWZ_2P_MBR_Exploris/experimental_design.json`
- Modify: `julia/metrics_report_test.jl`

- [ ] **Step 1: Restore no-MBR config**

Add `search_noMBR.json` using the current full dataset/library paths, current JSON shape, `match_between_runs: false`, and results path `/scratch2/fs1/d.goldfarb/pioneer-regression/search/EWZ_2P_MBR_Exploris_noMBR`. Add matching 16 CPU / 64 GB resources.

- [ ] **Step 2: Enable metrics and reporting**

Add `search_noMBR` runtime and identification metrics. Add `no_mbr_search: search_noMBR` to the false-transfer-rate config. Ensure report filtering includes the existing `ftr.precursors.*` metrics and nested `ftr.mbr_vs_no_mbr.*` metrics.

- [ ] **Step 3: Run focused tests**

Run:
`julia --project=julia julia/ftr_metrics_test.jl`
`julia --project=julia julia/metrics_report_parse_test.jl`
Expected: PASS.

### Task 4: Final Verification

**Files:**
- Review all modified files.

- [ ] **Step 1: Inspect diff**

Run: `git diff --check` and `git diff --stat`
Expected: no whitespace errors, scoped changes only.

- [ ] **Step 2: Run focused metric tests**

Run: `julia --project=julia julia/ftr_metrics_test.jl`
Expected: PASS.
