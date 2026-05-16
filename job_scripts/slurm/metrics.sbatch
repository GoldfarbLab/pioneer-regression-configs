#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=metrics
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=40:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.12.6
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/metrics.%j.out.txt
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/metrics.%j.out.txt
#SBATCH --exclude=c2-node-065

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"
: "${PIONEER_DATASET_NAME:?PIONEER_DATASET_NAME not set}"
RUN_DIR="${RUN_DIR%/}"

export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_DEPOT_PATH="${RUN_DIR}/julia-depot"
mkdir -p "$JULIA_DEPOT_PATH"
export ENTRAPMENT_ANALYSES_PATH="${RUN_DIR}/PioneerEntrapment.jl"
export PIONEER_DELETE_RESULTS="${PIONEER_DELETE_RESULTS:-true}"

echo "INFO: Starting metrics batch job at $(date -Is)"
echo "INFO: Host: $(hostname)"
echo "INFO: RUN_DIR=${RUN_DIR}"
echo "INFO: PIONEER_DATASET_NAME=${PIONEER_DATASET_NAME}"
echo "INFO: JULIA_NUM_THREADS=${JULIA_NUM_THREADS}"
echo "INFO: JULIA_DEPOT_PATH=${JULIA_DEPOT_PATH}"
echo "INFO: ENTRAPMENT_ANALYSES_PATH=${ENTRAPMENT_ANALYSES_PATH}"
echo "INFO: PIONEER_DELETE_RESULTS=${PIONEER_DELETE_RESULTS}"

metrics_project="${RUN_DIR}/regression-configs/julia"
metrics_script="${metrics_project}/regression_metrics.jl"
if [[ ! -f "$metrics_script" ]]; then
    echo "ERROR: regression metrics script not found at ${metrics_script}" >&2
    echo "Ensure the regression-configs repo is available at ${RUN_DIR}/regression-configs inside the container." >&2
    exit 1
fi

JULIA=/usr/local/julia/bin/julia
echo "INFO: Launching Julia metrics script at $(date -Is)"
echo "INFO: Julia executable=${JULIA}"
echo "INFO: Julia project=${metrics_project}"
echo "INFO: Metrics script=${metrics_script}"
if "$JULIA" --project="$metrics_project" "$metrics_script"; then
    echo "INFO: Finished Julia metrics script successfully at $(date -Is)"
else
    status=$?
    echo "ERROR: Julia metrics script failed with status ${status} at $(date -Is)" >&2
    exit "$status"
fi
