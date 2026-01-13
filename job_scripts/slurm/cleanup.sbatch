#!/bin/bash
#SBATCH --account=compute2-d.goldfarb
#SBATCH --job-name=pioneer-cleanup
#SBATCH --partition=general-cpu
#SBATCH --nodes=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=1:00:00
#SBATCH --container-image=dennisgoldfarb/julia-python:1.11.7
#SBATCH --container-mounts=/storage1/fs1/d.goldfarb/Active:/storage1/fs1/d.goldfarb/Active,/scratch2/fs1/d.goldfarb:/scratch2/fs1/d.goldfarb/
#SBATCH --output=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/pioneer-cleanup.%j.out
#SBATCH --error=/scratch2/fs1/d.goldfarb/pioneer-regression/logs/pioneer-cleanup.%j.out

set -euo pipefail
: "${RUN_DIR:?RUN_DIR not set}"
: "${RUN_SUFFIX:?RUN_SUFFIX not set}"

case "$RUN_DIR" in
  /storage1/fs1/d.goldfarb/Active/Automation/Pioneer/run-*) ;;
  *) echo "Refusing to clean unexpected run dir: $RUN_DIR" >&2; exit 1 ;;
esac

if [ "${RUN_DIR##*/}" != "$RUN_SUFFIX" ]; then
  echo "Run directory suffix mismatch: expected $RUN_SUFFIX, found ${RUN_DIR##*/}" >&2
  exit 1
fi

results_dir="$RUN_DIR/results"
if [ ! -d "$results_dir" ]; then
  echo "Results directory missing at $results_dir" >&2
  exit 1
fi

shopt -s dotglob nullglob
for entry in "$RUN_DIR"/* "$RUN_DIR"/.*; do
  base="$(basename "$entry")"
  if [ "$base" = "." ] || [ "$base" = ".." ] || [ "$entry" = "$results_dir" ]; then
    continue
  fi
  rm -rf -- "$entry"
done

echo "Cleanup complete; preserved results at $results_dir"
