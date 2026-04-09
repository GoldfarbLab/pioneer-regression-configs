#!/usr/bin/env julia

using Arrow
using DataFrames

include(joinpath(@__DIR__, "regression_metrics.jl"))

function write_test_tables(
    dataset_dir::AbstractString;
    abundance_values = Union{Missing, Float64}[10.0, missing, 5.0, missing],
    include_abundance::Bool = true,
)
    mkpath(dataset_dir)

    precursors_long = DataFrame(
        precursor = ["prec1", "prec2", "prec3"],
        global_qval = [0.01, 0.02, 0.03],
    )
    precursors_wide = DataFrame(
        precursor = ["prec1", "prec2"],
        global_qval = [0.01, 0.02],
        run1 = Union{Missing, Float64}[1.0, missing],
        run2 = Union{Missing, Float64}[2.0, 3.0],
    )
    protein_groups_long = if include_abundance
        DataFrame(
            protein_group = ["pg1", "pg1", "pg2", "pg3"],
            abundance = abundance_values,
        )
    else
        DataFrame(
            protein_group = ["pg1", "pg1", "pg2", "pg3"],
            score = [1.0, 2.0, 3.0, 4.0],
        )
    end
    protein_groups_wide = DataFrame(
        protein_group = ["pg1", "pg2", "pg3"],
        global_qval = [0.01, 0.02, 0.03],
        run1 = Union{Missing, Float64}[10.0, 5.0, missing],
        run2 = Union{Missing, Float64}[missing, 6.0, missing],
    )

    Arrow.write(joinpath(dataset_dir, "precursors_long.arrow"), precursors_long)
    Arrow.write(joinpath(dataset_dir, "precursors_wide.arrow"), precursors_wide)
    Arrow.write(joinpath(dataset_dir, "protein_groups_long.arrow"), protein_groups_long)
    Arrow.write(joinpath(dataset_dir, "protein_groups_wide.arrow"), protein_groups_wide)
end

function run_quantification_tests()
    mktempdir() do root
        mixed_dir = joinpath(root, "mixed")
        write_test_tables(mixed_dir)
        mixed_metrics = compute_dataset_metrics(
            mixed_dir,
            "MixedDataset";
            metric_groups = ["identification"],
        )

        @assert mixed_metrics["identification"]["protein_groups"]["total"] == 4
        @assert mixed_metrics["identification"]["protein_groups"]["unique"] == 3
        @assert !haskey(mixed_metrics["identification"]["protein_groups"], "complete_rows")
        @assert !haskey(mixed_metrics["identification"]["protein_groups"], "data_completeness")
        @assert mixed_metrics["quantification"]["protein_groups"]["total"] == 2
        @assert mixed_metrics["quantification"]["protein_groups"]["complete_rows"] == 1
        @assert mixed_metrics["quantification"]["protein_groups"]["data_completeness"] == 0.5

        all_missing_dir = joinpath(root, "all_missing")
        write_test_tables(
            all_missing_dir;
            abundance_values = Union{Missing, Float64}[missing, missing, missing, missing],
        )
        all_missing_metrics = compute_dataset_metrics(
            all_missing_dir,
            "AllMissingDataset";
            metric_groups = ["identification"],
        )

        @assert all_missing_metrics["quantification"]["protein_groups"]["total"] == 0
        @assert all_missing_metrics["identification"]["protein_groups"]["total"] == 4

        missing_abundance_dir = joinpath(root, "missing_abundance")
        write_test_tables(missing_abundance_dir; include_abundance = false)
        missing_abundance_metrics = compute_dataset_metrics(
            missing_abundance_dir,
            "MissingAbundanceDataset";
            metric_groups = ["identification"],
        )

        @assert !haskey(missing_abundance_metrics["quantification"]["protein_groups"], "total")
        @assert missing_abundance_metrics["quantification"]["protein_groups"]["complete_rows"] == 1
        @assert missing_abundance_metrics["quantification"]["protein_groups"]["data_completeness"] == 0.5
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_quantification_tests()
    println("quantification metrics tests passed.")
end
