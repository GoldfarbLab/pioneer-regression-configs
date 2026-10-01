# SearchDIA for one regression param file (search_dia.bsub / slurm/search_dia.sbatch).
#
# A dataset stored as vendor raw data carries a conversion config in params/<dataset>/:
#   convert_bruker.json  {"input": "<folder of .d>"}
#   convert_sciex.json   {"input": "<folder of .wiff + .wiff.scan>", "zt_scan": true | false}
#                        (zt_scan is required: the .wiff does not record whether a run is ZT Scan DIA)
# Its runs are converted first, with the Pioneer under test (so every regression run also tests the converter and
# never depends on a stored .tdfs / .scxs format), into RUN_DIR/converted/<dataset>/<param stem>/, and the search
# reads them from there: the param file's paths.ms_data is replaced in a copy (RUN_DIR/converted/<dataset>/<param
# stem>_params.json). Other datasets search as before.
using Pioneer, JSON

param_file = ENV["PARAM_FILE"]
dataset = ENV["PIONEER_DATASET_NAME"]
config_dir = joinpath(ENV["RUN_DIR"], "regression-configs", "params", dataset)
out = joinpath(ENV["RUN_DIR"], "converted", dataset, splitext(basename(param_file))[1])
converted = if isfile(joinpath(config_dir, "convert_bruker.json"))
    input = JSON.parsefile(joinpath(config_dir, "convert_bruker.json"))["input"]
    t = @elapsed runs = convertBruker(input; output_dir = out)
    println("Converted $(length(runs)) Bruker run(s) from $input to $out in $(round(t; digits = 1)) s")
    true
elseif isfile(joinpath(config_dir, "convert_sciex.json"))
    cfg = JSON.parsefile(joinpath(config_dir, "convert_sciex.json"))
    t = @elapsed runs = convertSciex(cfg["input"]; output_dir = out, zt_scan = Bool(cfg["zt_scan"]))
    println("Converted $(length(runs)) SCIEX run(s) (zt_scan = $(cfg["zt_scan"])) from $(cfg["input"]) to $out in ",
            "$(round(t; digits = 1)) s")
    true
else
    false
end
if converted
    params = JSON.parsefile(param_file)
    params["paths"]["ms_data"] = out
    # next to the converted runs, not in adjusted-params: the metrics job reads every search*.json there as a search
    param_file = out * "_params.json"
    write(param_file, JSON.json(params, 2))
end
SearchDIA(param_file)
