#!/usr/bin/env julia

using Arrow
using DataFrames

include(joinpath(@__DIR__, "regression_metrics.jl"))

function write_ftr_tables(
    dataset_dir::AbstractString,
    precursor_wide::DataFrame,
    protein_long::DataFrame,
    protein_wide::DataFrame,
)
    mkpath(dataset_dir)
    Arrow.write(joinpath(dataset_dir, "precursors_wide.arrow"), precursor_wide)
    Arrow.write(joinpath(dataset_dir, "protein_groups_long.arrow"), protein_long)
    Arrow.write(joinpath(dataset_dir, "protein_groups_wide.arrow"), protein_wide)
end

function run_ftr_metrics_tests()
    mktempdir() do root
        precursor_combined = DataFrame(
            precursor = ["p1", "p2"],
            species = ["YEAST", "HUMAN"],
            global_qval = [0.01, 0.02],
            run1 = Union{Missing, Float64}[1.0, missing],
            run2 = Union{Missing, Float64}[missing, 1.0],
        )
        precursor_human_only = DataFrame(
            precursor = ["p2"],
            species = ["HUMAN"],
            global_qval = [0.02],
            run1 = Union{Missing, Float64}[1.0],
        )
        precursor_human_yeast = DataFrame(
            precursor = ["p3"],
            species = ["HUMAN"],
            global_qval = [0.03],
            run2 = Union{Missing, Float64}[1.0],
        )

        protein_long_combined = DataFrame(
            file_name = ["run1", "run1", "run2", "run2"],
            species = ["YEAST", "HUMAN", "HUMAN", "YEAST"],
            protein = ["pg_y_honly", "pg_h_honly", "pg_h_hy", "pg_y_hy"],
            abundance = Union{Missing, Float64}[missing, missing, missing, missing],
        )
        protein_long_human_only = DataFrame(
            file_name = ["run1"],
            species = ["HUMAN"],
            protein = ["pg_h_honly"],
            abundance = Union{Missing, Float64}[missing],
        )
        protein_long_human_yeast = DataFrame(
            file_name = ["run2", "run2"],
            species = ["HUMAN", "YEAST"],
            protein = ["pg_h_hy", "pg_y_hy"],
            abundance = Union{Missing, Float64}[missing, missing],
        )

        # These wide tables intentionally carry no quant values so the test would fail
        # if protein FTR still used quantified-wide presence instead of long-table IDs.
        protein_wide_combined = DataFrame(
            protein = ["pg_y_honly", "pg_h_honly", "pg_h_hy", "pg_y_hy"],
            species = ["YEAST", "HUMAN", "HUMAN", "YEAST"],
            global_qval = [0.01, 0.02, 0.03, 0.04],
            run1 = Union{Missing, Float64}[missing, missing, missing, missing],
            run2 = Union{Missing, Float64}[missing, missing, missing, missing],
        )
        protein_wide_human_only = DataFrame(
            protein = ["pg_h_honly"],
            species = ["HUMAN"],
            global_qval = [0.02],
            run1 = Union{Missing, Float64}[missing],
        )
        protein_wide_human_yeast = DataFrame(
            protein = ["pg_h_hy", "pg_y_hy"],
            species = ["HUMAN", "YEAST"],
            global_qval = [0.03, 0.04],
            run2 = Union{Missing, Float64}[missing, missing],
        )

        combined_dir = joinpath(root, "combined")
        human_only_dir = joinpath(root, "human_only")
        human_yeast_dir = joinpath(root, "human_yeast")

        write_ftr_tables(combined_dir, precursor_combined, protein_long_combined, protein_wide_combined)
        write_ftr_tables(human_only_dir, precursor_human_only, protein_long_human_only, protein_wide_human_only)
        write_ftr_tables(human_yeast_dir, precursor_human_yeast, protein_long_human_yeast, protein_wide_human_yeast)

        experimental_design = Dict{String, Any}(
            "FTRDataset" => Dict{String, Any}(
                "runs" => Dict{String, String}(
                    "run1" => "human_only",
                    "run2" => "human_yeast",
                ),
                "false_transfer_rate" => Dict{String, String}(
                    "combined_search" => "search_combined",
                    "human_only_search" => "search_human_only",
                    "human_yeast_search" => "search_human_yeast",
                ),
            ),
        )

        metrics = compute_ftr_metrics(
            "FTRDataset",
            "search_combined",
            precursor_combined,
            protein_long_combined,
            protein_wide_combined,
            experimental_design,
            Dict(
                "search_combined" => combined_dir,
                "search_human_only" => human_only_dir,
                "search_human_yeast" => human_yeast_dir,
            ),
        )

        @assert metrics["protein_groups"]["additional_yeast_IDs"] == 1
        @assert metrics["protein_groups"]["additional_IDs"] == 1
        @assert metrics["protein_groups"]["false_transfer_rate"] == 1.0

        # Precursors still use the wide-table path.
        @assert metrics["precursors"]["additional_yeast_IDs"] == 1
        @assert metrics["precursors"]["additional_IDs"] == 0
        @assert metrics["precursors"]["false_transfer_rate"] == 0.0
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_ftr_metrics_tests()
    println("ftr metrics tests passed.")
end
