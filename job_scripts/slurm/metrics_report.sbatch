#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=report
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=8G
#SBATCH --time=10:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.11.7
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/report.%j.out.txt
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/report.%j.out.txt

set -euo pipefail

: "${RUN_DIR:?RUN_DIR not set}"

export JULIA_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export JULIA_DEPOT_PATH=/storage1/fs1/d.goldfarb/Active/Automation/Pioneer/julia-depot

repo_dir="${RUN_DIR%/}/regression-configs"
report_project="${repo_dir}/julia"
report_script="${report_project}/metrics_report.jl"
if [[ ! -f "$report_script" ]]; then
    echo "ERROR: metrics report script not found at ${report_script}" >&2
    echo "Ensure the regression-configs repo is available at ${repo_dir} inside the container." >&2
    exit 1
fi

JULIA=/usr/local/julia/bin/julia
"$JULIA" --project="$report_project" "$report_script"
