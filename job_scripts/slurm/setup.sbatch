#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=setup
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=16G
#SBATCH --time=24:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.12.6
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/setup.%j.out
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/setup.%j.out
#SBATCH --exclude=c2-node-065

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"
RUN_DIR="${RUN_DIR%/}"
PIONEER_DIR="${RUN_DIR}/pioneer"
PIONEER_ENTRAPMENT_DIR="${RUN_DIR}/PioneerEntrapment.jl"
export PIONEER_DIR PIONEER_ENTRAPMENT_DIR

export JULIA_DEPOT_PATH="${RUN_DIR}/julia-depot"
export JULIA_NUM_PRECOMPILE_TASKS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

mkdir -p "$JULIA_DEPOT_PATH"

metrics_project="${RUN_DIR}/regression-configs/julia"

JULIA=/usr/local/julia/bin/julia

echo "Precompiling Pioneer project"
if [[ ! -d "$PIONEER_DIR" ]]; then
    echo "ERROR: Pioneer project not found at ${PIONEER_DIR}" >&2
    exit 1
fi
"$JULIA" --project="$PIONEER_DIR" -e '
    using Pkg
    Pkg.instantiate()
    Pkg.precompile()
'

echo "Precompiling PioneerEntrapment project"
if [[ ! -d "$PIONEER_ENTRAPMENT_DIR" ]]; then
    echo "ERROR: PioneerEntrapment project not found at ${PIONEER_ENTRAPMENT_DIR}" >&2
    exit 1
fi
"$JULIA" --project="$PIONEER_ENTRAPMENT_DIR" -e '
    using Pkg
    Pkg.instantiate()
    Pkg.precompile()
'

echo "Precompiling regression metrics project"
if [[ ! -d "$metrics_project" ]]; then
    echo "ERROR: regression metrics project not found at ${metrics_project}" >&2
    exit 1
fi
"$JULIA" --project="$metrics_project" -e '
    using Pkg
    Pkg.instantiate()
    Pkg.precompile()
'
