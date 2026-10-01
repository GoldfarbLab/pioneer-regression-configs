# SearchDIA for one regression param file (search_dia.bsub / slurm/search_dia.sbatch).
#
# A dataset stored as vendor raw data carries params/<dataset>/convert_bruker.json, {"input": "<folder of .d>"}.
# Its runs are converted first, with the Pioneer under test (so every regression run also tests the converter and
# never depends on a stored .tdfs format), into RUN_DIR/converted/<dataset>/<param stem>/, and the search reads
# them from there: the param file's paths.ms_data is replaced in a copy (RUN_DIR/converted/<dataset>/<param stem>_params.json).
# Other datasets search as before.
using Pioneer, JSON

param_file = ENV["PARAM_FILE"]
dataset = ENV["PIONEER_DATASET_NAME"]
convert_cfg = joinpath(ENV["RUN_DIR"], "regression-configs", "params", dataset, "convert_bruker.json")
if isfile(convert_cfg)
    input = JSON.parsefile(convert_cfg)["input"]
    out = joinpath(ENV["RUN_DIR"], "converted", dataset, splitext(basename(param_file))[1])
    t = @elapsed tdfs = convertBruker(input; output_dir = out)
    println("Converted $(length(tdfs)) Bruker run(s) from $input to $out in $(round(t; digits = 1)) s")
    params = JSON.parsefile(param_file)
    params["paths"]["ms_data"] = out
    # next to the converted runs, not in adjusted-params: the metrics job reads every search*.json there as a search
    param_file = out * "_params.json"
    write(param_file, JSON.json(params, 2))
end
SearchDIA(param_file)
