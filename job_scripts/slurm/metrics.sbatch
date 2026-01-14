#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=metrics
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=40:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.11.7
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/metrics.%j.out.txt
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/metrics.%j.out.txt

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"
: "${PIONEER_DATASET_NAME:?PIONEER_DATASET_NAME not set}"
RUN_DIR="${RUN_DIR%/}"

export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_DEPOT_PATH=/storage1/fs1/d.goldfarb/Active/Automation/Pioneer/julia-depot
export ENTRAPMENT_ANALYSES_PATH="${RUN_DIR}/PioneerEntrapment.jl"

metrics_project="${RUN_DIR}/regression-configs/julia"
metrics_script="${metrics_project}/regression_metrics.jl"
if [[ ! -f "$metrics_script" ]]; then
    echo "ERROR: regression metrics script not found at ${metrics_script}" >&2
    echo "Ensure the regression-configs repo is available at ${RUN_DIR}/regression-configs inside the container." >&2
    exit 1
fi

JULIA=/usr/local/julia/bin/julia
"$JULIA" --project="$metrics_project" "$metrics_script"
