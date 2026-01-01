#!/usr/bin/env julia

include(joinpath(@__DIR__, "metrics_report.jl"))

function run_parse_tests()
    root = "/tmp/pioneer-metrics-root"
    paths = [
        joinpath(root, "results", "Example_Dataset", "metrics_Example_Dataset_search_alpha.json"),
        joinpath(root, "Example_Dataset", "metrics_Example_Dataset_search_alpha.json"),
        joinpath(root, "results", "MTAC_Yeast_Alternating_5min", "metrics_MTAC_Yeast_Alternating_5min_search_entrap.json"),
    ]

    dataset, search = parse_dataset_search(paths[1], root)
    @assert dataset == "Example_Dataset"
    @assert search == "search_alpha"

    dataset, search = parse_dataset_search(paths[2], root)
    @assert dataset == "Example_Dataset"
    @assert search == "search_alpha"

    dataset, search = parse_dataset_search(paths[3], root)
    @assert dataset == "MTAC_Yeast_Alternating_5min"
    @assert search == "search_entrap"
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_parse_tests()
    println("metrics_report parse tests passed.")
end
