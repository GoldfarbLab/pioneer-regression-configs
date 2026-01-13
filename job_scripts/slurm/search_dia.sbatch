#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=regression-search
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=50:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.11.7
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/regression-search.%j.out.txt
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/regression-search.%j.out.txt

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"
: "${PARAM_FILE:?PARAM_FILE not set}"
: "${PIONEER_DATASET_NAME:?PIONEER_DATASET_NAME not set}"
RUN_DIR="${RUN_DIR%/}"
PIONEER_DIR="${RUN_DIR}/pioneer"
PARAM_FILE="${RUN_DIR}/regression-configs/params/${PIONEER_DATASET_NAME}/${PARAM_FILE}"

export PIONEER_DIR
JULIA=/usr/local/julia/bin/julia

export JULIA_DEPOT_PATH=/storage1/fs1/d.goldfarb/Active/Automation/Pioneer/julia-depot
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OMP_THREAD_LIMIT="${SLURM_CPUS_PER_TASK:-1}"
export OMP_DYNAMIC=FALSE
export OPENBLAS_NUM_THREADS=1

"$JULIA" --project="$PIONEER_DIR" -e '
    using Pioneer
    SearchDIA(ENV["PARAM_FILE"])
'
