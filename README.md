# pioneer-regression-configs

This repository holds regression configuration files and batch scripts used to
run Pioneer search and metrics workflows, along with the Julia code that
computes regression metrics and reports.

## Repository layout

- `params/`: per-dataset search parameter JSON files used for regression runs.
- `params-exclude/`: parameter files that are excluded from default runs.
- `job_scripts/`: LSF batch scripts for setup, search, metrics, and cleanup, plus
  SLURM equivalents in `job_scripts/slurm/`.
- `julia/`: Julia package and scripts for regression metrics and reports.

## Running regression metrics

The regression metrics entrypoint is `julia/regression_metrics.jl`. It expects a
results root directory containing search outputs. Provide the location via the
`PIONEER_RESULTS_DIR` environment variable or pass it as the first argument.

Example:

```bash
export PIONEER_RESULTS_DIR=/scratch2/fs1/d.goldfarb/pioneer-regression/search
julia julia/regression_metrics.jl
```

Set `PIONEER_PRESERVE_RESULTS=true` to skip cleanup and copy full search results into
the storage-backed `results/` directory (the original search outputs remain in place).
By default, only logs, metrics, and QC artifacts are retained.

The script uses optional JSON configuration for metric grouping. If a metrics
config is not provided or cannot be parsed, default metric groups are used.

## Regression metrics report layouts

The metrics report (`julia/metrics_report.jl`) supports two directory layouts:

- `<root>/results/<dataset>/metrics_<dataset>_<search>.json`
- `<root>/<dataset>/metrics_<dataset>_<search>.json`

Search names are always derived from the filename suffix after the last
underscore so dataset names can include underscores (for example,
`metrics_MTAC_Yeast_Alternating_5min_search_entrap.json` parses as dataset
`MTAC_Yeast_Alternating_5min` and search `search_entrap`). Set
`PIONEER_METRICS_RELEASE_ROOT`, `PIONEER_METRICS_DEVELOP_ROOT`, and
`PIONEER_METRICS_CURRENT_ROOT` to the appropriate root for each version (these
can differ between release/develop/current depending on how results were
archived).

## Batch scripts

The scripts in `job_scripts/` are intended for LSF execution, while
`job_scripts/slurm/` contains SLURM equivalents that wrap Julia/Python entrypoints
with `srun --container-image`. They use `PIONEER_REGRESSION_CONFIGS_DIR` to locate
this repository when running inside containers and expect Pioneer results under
the standard regression paths in `/scratch2/fs1/d.goldfarb/pioneer-regression/`.

## Dataset tags

The `params/dataset_tags.json` file maps each dataset directory name under `params/` to a set of tags.

Schema:

```json
{
  "datasets": {
    "<dataset_dir>": { "tags": ["fast"], "order": 100 }
  }
}
```

To mark a dataset as fast, add the string `"fast"` to its `tags` array.
Use `"order"` to specify an explicit submission ordering value (higher runs
earlier); omit the field or set it to `0` to use the default ordering.
