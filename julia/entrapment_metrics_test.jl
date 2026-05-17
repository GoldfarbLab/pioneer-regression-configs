#!/usr/bin/env julia

using Arrow
using DataFrames

include(joinpath(@__DIR__, "metrics", "entrapment_metrics.jl"))

function run_entrapment_metrics_tests()
    mktempdir() do root
        precursors_path = joinpath(root, "precursors_long.arrow")
        Arrow.write(
            precursors_path,
            DataFrame(
                precursor_idx = UInt32[1, 2],
                target = Bool[true, true],
                global_prob = Float32[0.9, 0.8],
                global_qval = Float32[0.001, 0.002],
                prec_prob = Float32[0.85, 0.75],
                qval = Float32[0.003, 0.004],
            ),
        )

        expected_pairs = [
            (:global_prob, :global_qval),
            (:prec_prob, :qval),
        ]

        @assert EntrapmentMetrics.precursor_score_pairs(
            precursors_path;
            match_between_runs = true,
        ) == expected_pairs
        @assert EntrapmentMetrics.precursor_score_pairs(
            precursors_path;
            match_between_runs = false,
        ) == expected_pairs
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_entrapment_metrics_tests()
    println("entrapment metrics tests passed.")
end
