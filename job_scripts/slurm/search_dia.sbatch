#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=search
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=16G
#SBATCH --time=50:00:00
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/search.%j.out.txt
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/search.%j.out.txt

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"
: "${PARAM_FILE:?PARAM_FILE not set}"
: "${PIONEER_DATASET_NAME:?PIONEER_DATASET_NAME not set}"
: "${SLURM_CPUS_PER_TASK:?SLURM_CPUS_PER_TASK not set}"
RUN_DIR="${RUN_DIR%/}"
PIONEER_DIR="${RUN_DIR}/pioneer"
PARAM_FILE="${RUN_DIR}/adjusted-params/${PIONEER_DATASET_NAME}/${PARAM_FILE}"

export RUN_DIR PIONEER_DATASET_NAME PIONEER_DIR PARAM_FILE
JULIA=/usr/local/julia/bin/julia

export JULIA_DEPOT_PATH="${RUN_DIR}/julia-depot"
mkdir -p "$JULIA_DEPOT_PATH"
export JULIA_NUM_THREADS="$SLURM_CPUS_PER_TASK"

export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export BLIS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export OMP_NUM_THREADS="$SLURM_CPUS_PER_TASK"
export OMP_THREAD_LIMIT="$SLURM_CPUS_PER_TASK"
export OMP_DYNAMIC=FALSE
export OMP_PLACES=cores
export OMP_PROC_BIND=close

echo "INFO: SLURM job ID: ${SLURM_JOB_ID:-unset}"
echo "INFO: Hostname: $(hostname)"
echo "INFO: Requested CPU count: $SLURM_CPUS_PER_TASK"
echo "INFO: Cpus_allowed_list: $(awk '/^Cpus_allowed_list:/ { print $2 }' /proc/self/status)"
echo "INFO: $(taskset -pc $$)"
echo "INFO: nproc: $(nproc)"
echo "INFO: nproc --all: $(nproc --all)"

srun \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task="$SLURM_CPUS_PER_TASK" \
    --cpu-bind=verbose,cores \
    --export=ALL \
    --container-image=dennisgoldfarb/julia-python:1.12.6 \
    --container-mounts=/storage3/fs1/d.goldfarb/Active:/storage3/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/ \
    --container-env=RUN_DIR,PIONEER_DATASET_NAME,PIONEER_DIR,PARAM_FILE,JULIA_DEPOT_PATH,JULIA_NUM_THREADS,OPENBLAS_NUM_THREADS,MKL_NUM_THREADS,BLIS_NUM_THREADS,NUMEXPR_NUM_THREADS,OMP_NUM_THREADS,OMP_THREAD_LIMIT,OMP_DYNAMIC,OMP_PLACES,OMP_PROC_BIND \
    "$JULIA" --project="$PIONEER_DIR" "${RUN_DIR}/regression-configs/job_scripts/search_dia.jl"
