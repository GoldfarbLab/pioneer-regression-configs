#!/usr/bin/env julia

using DataFrames

include(joinpath(@__DIR__, "regression_metrics.jl"))

function run_fold_change_metrics_tests()
    design = (
        run_to_condition = Dict(
            "a1" => "A",
            "a2" => "A",
            "b1" => "B",
            "b2" => "B",
        ),
        condition_pairs = [
            (
                numerator = "A",
                denominator = "B",
                expected = Dict("HUMAN" => 1.0),
            ),
        ],
    )

    df = DataFrame(
        species = ["HUMAN", "HUMAN", "HUMAN"],
        a1 = Union{Missing, Float64}[8.0, missing, 8.0],
        a2 = Union{Missing, Float64}[8.0, 16.0, 8.0],
        b1 = Union{Missing, Float64}[4.0, 4.0, missing],
        b2 = Union{Missing, Float64}[4.0, 4.0, 2.0],
    )

    metrics = fold_change_metrics_for_table(
        df,
        [:a1, :a2, :b1, :b2],
        design,
        design.condition_pairs;
        table_label = "fold_change_test",
    )

    human_metrics = metrics["A_over_B"]
    @assert human_metrics["human_median_deviation"] == 1.0
    @assert human_metrics["human_fc_variance"] == 0.0
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_fold_change_metrics_tests()
    println("fold-change metrics tests passed.")
end
