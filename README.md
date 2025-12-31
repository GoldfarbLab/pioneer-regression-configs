# pioneer-regression-configs

This repository holds regression configuration files and batch scripts used to
run Pioneer search and metrics workflows, along with the Julia code that
computes regression metrics and reports.

## Repository layout

- `params/`: per-dataset search parameter JSON files used for regression runs.
- `params-exclude/`: parameter files that are excluded from default runs.
- `job_scripts/`: LSF batch scripts for setup, search, metrics, and cleanup.
- `julia/`: Julia package and scripts for regression metrics and reports.

## Running regression metrics

The regression metrics entrypoint is `julia/regression_metrics.jl`. It expects a
results root directory containing search outputs. Provide the location via the
`PIONEER_RESULTS_DIR` environment variable or pass it as the first argument.

Example:

```bash
export PIONEER_RESULTS_DIR=/scratch1/fs1/d.goldfarb/pioneer-regression/search
julia julia/regression_metrics.jl
```

The script uses optional JSON configuration for metric grouping. If a metrics
config is not provided or cannot be parsed, default metric groups are used.

## Batch scripts

The scripts in `job_scripts/` are intended for LSF execution. They use
`PIONEER_REGRESSION_CONFIGS_DIR` to locate this repository when running inside
containers and expect Pioneer results under the standard regression paths in
`/scratch1/fs1/d.goldfarb/pioneer-regression/`.

## Dataset tags

The `params/dataset_tags.json` file maps each dataset directory name under `params/` to a set of tags.

Schema:

```json
{
  "datasets": {
    "<dataset_dir>": { "tags": ["fast"] }
  }
}
```

To mark a dataset as fast, add the string `"fast"` to its `tags` array.
