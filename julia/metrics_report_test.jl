#!/usr/bin/env julia

include(joinpath(@__DIR__, "metrics_report.jl"))

function report_index(report::AbstractString, needle::AbstractString)
    index = findfirst(needle, report)
    index === nothing && error("Missing expected report content: " * needle)
    first(index)
end

function run_metrics_report_tests()
    versions = ["v0.6.4", "current"]

    report = build_report(
        Dict{String, Any}(
            "v0.6.4" => Dict{String, Any}(
                "search" => Dict{String, Any}(
                    "ExampleDataset" => Dict{String, Any}(
                        "identification.precursors.total" => 10.0,
                        "cv.protein_groups.median_cv" => 0.25,
                        "fold_change.error.precursors.a_over_b.human_median_deviation" => 1.2,
                        "fold_change.variance.precursors.a_over_b.human_fc_variance" => 0.3,
                        "runtime.runtime_minutes" => 15.0,
                    ),
                ),
            ),
            "current" => Dict{String, Any}(
                "search" => Dict{String, Any}(
                    "ExampleDataset" => Dict{String, Any}(
                        "identification.precursors.total" => 12.0,
                        "cv.protein_groups.median_cv" => 0.20,
                        "fold_change.error.precursors.a_over_b.human_median_deviation" => 0.8,
                        "fold_change.variance.precursors.a_over_b.human_fc_variance" => 0.2,
                        "runtime.runtime_minutes" => 11.0,
                    ),
                ),
            ),
        ),
        versions,
    )

    identification_index = report_index(report, "<h3>Metric: identification.precursors.total</h3>")
    cv_index = report_index(report, "<h3>Metric: cv.protein_groups.median_cv</h3>")
    fold_change_index = report_index(report, "<h3>fold_change.error.precursors</h3>")
    fold_change_variance_index = report_index(report, "<h3>fold_change.variance.precursors</h3>")
    runtime_index = report_index(report, "<h3>Metric: runtime.runtime_minutes</h3>")

    @assert identification_index < cv_index
    @assert cv_index < fold_change_index
    @assert fold_change_index < fold_change_variance_index
    @assert fold_change_variance_index < runtime_index

    unknown_report = build_report(
        Dict{String, Any}(
            "current" => Dict{String, Any}(
                "search" => Dict{String, Any}(
                    "ExampleDataset" => Dict{String, Any}(
                        "mystery.metric" => 1.0,
                        "runtime.runtime_minutes" => 10.0,
                    ),
                ),
            ),
        ),
        ["current"],
    )

    runtime_unknown_index = report_index(unknown_report, "<h3>Metric: runtime.runtime_minutes</h3>")
    mystery_index = report_index(unknown_report, "<h3>Metric: mystery.metric</h3>")

    @assert runtime_unknown_index < mystery_index
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_metrics_report_tests()
    println("metrics report tests passed.")
end
