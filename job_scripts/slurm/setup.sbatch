#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=pioneer-setup
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=24:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.11.7
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/pioneer-setup.%j.out
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/pioneer-setup.%j.out

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"
RUN_DIR="${RUN_DIR%/}"
PIONEER_DIR="${RUN_DIR}/pioneer"
PIONEER_ENTRAPMENT_DIR="${RUN_DIR}/PioneerEntrapment.jl"
export PIONEER_DIR PIONEER_ENTRAPMENT_DIR

export JULIA_DEPOT_PATH=/storage1/fs1/d.goldfarb/Active/Automation/Pioneer/julia-depot
export JULIA_NUM_PRECOMPILE_TASKS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

mkdir -p "$JULIA_DEPOT_PATH"

metrics_project="${RUN_DIR}/regression-configs/julia"
config_root="${RUN_DIR}/regression-configs"
scratch_root="/scratch2/fs1/d.goldfarb/pioneer-regression/"
storage_root="/storage1/fs1/d.goldfarb/Active/Automation/Pioneer/"

JULIA=/usr/local/julia/bin/julia

has_non_dir_files() {
    local target_dir="$1"
    find "$target_dir" -maxdepth 1 -type f -print -quit | grep -q .
}

copy_from_storage() {
    local scratch_path="$1"
    local storage_path="${scratch_path/#$scratch_root/$storage_root}"

    if [[ "$scratch_path" == "$storage_path" ]]; then
        echo "WARNING: $scratch_path does not use scratch root; skipping copy." >&2
        return 0
    fi

    if [[ -d "$storage_path" ]]; then
        mkdir -p "$scratch_path"
        cp -a --no-preserve=mode,ownership "$storage_path"/. "$scratch_path"/
    elif [[ -f "$storage_path" ]]; then
        mkdir -p "$(dirname "$scratch_path")"
        cp -a --no-preserve=mode,ownership "$storage_path" "$scratch_path"
    else
        echo "WARNING: storage source missing for $scratch_path ($storage_path)" >&2
    fi
}

if [[ -d "$config_root" ]]; then
    echo "Scanning search configs under ${config_root} for ms_data/library paths."
    if [[ ! -f "${config_root}/scripts/collect_search_paths.py" ]]; then
        echo "ERROR: missing ${config_root}/scripts/collect_search_paths.py" >&2
        exit 1
    fi
    mapfile -t data_paths < <(python "${config_root}/scripts/collect_search_paths.py" "$config_root")
    echo "Found ${#data_paths[@]} unique data/library paths to verify."

    for scratch_path in "${data_paths[@]}"; do
        if [[ -f "$scratch_path" ]]; then
            echo "Verified file exists: $scratch_path"
            continue
        fi

        if [[ -d "$scratch_path" ]] && has_non_dir_files "$scratch_path"; then
            echo "Verified directory has files: $scratch_path"
            continue
        fi

        echo "Populating scratch path: $scratch_path"
        echo "  -> storage source: ${scratch_path/#$scratch_root/$storage_root}"
        copy_from_storage "$scratch_path"
    done
else
    echo "WARNING: config root not found at ${config_root}" >&2
fi

"$JULIA" --project="$PIONEER_DIR" -e '
    using Pkg
    Pkg.develop(path=ENV["PIONEER_ENTRAPMENT_DIR"])
    Pkg.instantiate()
    Pkg.precompile()
'

if [[ -d "$metrics_project" ]]; then
    "$JULIA" --project="$metrics_project" -e '
        using Pkg
        Pkg.instantiate()
        Pkg.precompile()
    '
else
    echo "WARNING: regression metrics project not found at ${metrics_project}" >&2
fi
