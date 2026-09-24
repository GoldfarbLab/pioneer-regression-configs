#!/usr/bin/env julia

using Test
using Logging

include(joinpath(@__DIR__, "regression_metrics.jl"))

function with_metric_warnings(f::Function)
    logger = Test.TestLogger(min_level = Logging.Warn)
    result = with_logger(f, logger)
    result, logger.logs
end

function test_unavailable_cv(df, quant_columns; groups = Dict{String, Vector{String}}())
    result, logs = with_metric_warnings() do
        compute_cv_metrics(df, quant_columns; groups = groups)
    end
    @test result.median_cv === nothing
    @test any(log -> occursin("CV unavailable", log.message), logs)
    @test JSON.parse(JSON.json(result))["median_cv"] === nothing
    result
end

function run_matching_metrics_tests()
    full_names = ["20230324_lab.v2_A_01", "20230324_lab.v2_A_02",
                  "20230324_lab.v2_B_01", "20230324_lab.v2_B_02"]
    mapping = Dict(full_names[1] => "A", full_names[2] => "A",
                   full_names[3] => "B", full_names[4] => "B")
    groups = Dict("A" => full_names[1:2], "B" => full_names[3:4])
    design = (run_to_condition = mapping,
              condition_pairs = [(numerator = "A", denominator = "B",
                                  expected = Dict("HUMAN" => 1.0))])
    df = DataFrame(species = ["HUMAN", "HUMAN"], global_qval = [0.01, 0.01])
    for name in full_names[1:2]
        df[!, name] = [8.0, 16.0]
    end
    for name in full_names[3:4]
        df[!, name] = [4.0, 4.0]
    end

    @testset "Exact full run names" begin
        for names in (full_names, Symbol.(full_names))
            @test resolve_run_columns(df, names, full_names) == full_names
            @test resolve_run_columns(df, names, nothing) == full_names
            @test resolve_run_columns(df, names, [full_names[1], full_names[1]]) == full_names[1:1]
            conditions, missing_runs = condition_columns(names, mapping)
            @test isempty(missing_runs)
            @test Set(String.(conditions["A"])) == Set(groups["A"])
            @test Set(String.(conditions["B"])) == Set(groups["B"])
        end
        # Neither a unique suffix nor an ambiguous suffix is an explicit run ID.
        for available in ([full_names[1]], [full_names[1], "other_lab_A_01"])
            ambiguous_df = DataFrame([name => [1.0] for name in available])
            @test isempty(resolve_run_columns(ambiguous_df, available, ["A_01"]))
            conditions, missing_runs = condition_columns(available, Dict("A_01" => "A"))
            @test isempty(conditions)
            @test missing_runs == ["A_01"]
            test_unavailable_cv(ambiguous_df, available; groups = Dict("A" => ["A_01", "A_02"]))
        end
        @test isempty(resolve_run_columns(df, full_names, [full_names[1] * ".arrow"]))
        @test run_groups_for_dataset(Dict{String, Any}("runs" => mapping, "composition" => groups), "test") == groups
    end

    @testset "Valid CV and fold change" begin
        result = compute_cv_metrics(df, full_names; groups = groups)
        @test result.median_cv === 0.0
        @test result.runs == 4
        @test result.rows_evaluated == 4
        fold_change = fold_change_metrics_for_table(df, full_names, design, design.condition_pairs;
                                                    table_label = "test")
        @test fold_change["A_over_B"]["human_median_deviation"] == 1.5
        @test fold_change["A_over_B"]["human_fc_variance"] == 0.25

        varying = copy(df)
        varying[!, full_names[2]] .*= 2
        varying[!, full_names[4]] .*= 2
        @test compute_cv_metrics(varying, full_names; groups = groups).median_cv ≈ sqrt(2) / 3
        @test compute_cv_metrics(varying, full_names[1:2]).median_cv ≈ sqrt(2) / 3
    end

    @testset "Unavailable CVs" begin
        test_unavailable_cv(df, String[])
        test_unavailable_cv(df, full_names[1:1])
        test_unavailable_cv(df[1:0, :], full_names)
        test_unavailable_cv(df, [full_names; "absent"])
        test_unavailable_cv(df, full_names; groups = Dict("A" => ["absent1", "absent2"]))
        test_unavailable_cv(df, full_names; groups = Dict("A" => [full_names[1], "absent"]))
        # Even with two matching replicates, a configured third cannot disappear silently.
        test_unavailable_cv(df, full_names; groups = Dict("A" => [full_names[1:2]; "absent"]))
        test_unavailable_cv(df, full_names; groups = Dict("A" => [full_names[1], full_names[1]]))
        test_unavailable_cv(df, full_names; groups = Dict("A" => [full_names[1:2]; full_names[1]]))
        test_unavailable_cv(df, full_names; groups = Dict("A" => String[]))
        test_unavailable_cv(df, full_names; groups = Dict("A" => groups["A"], "B" => ["absent"]))
        # Only quantification columns are eligible, even if a requested metadata column exists.
        test_unavailable_cv(df, full_names; groups = Dict("A" => [full_names[1], "global_qval"]))
        for values in ([missing, missing], [0.0, 0.0], [NaN, NaN], [Inf, Inf], [-1.0, 1.0])
            invalid = DataFrame(r1 = [values[1]], r2 = [values[2]])
            test_unavailable_cv(invalid, ["r1", "r2"])
        end
        mixed = DataFrame(r1 = [missing, 2.0, Inf], r2 = [1.0, 2.0, Inf])
        @test compute_cv_metrics(mixed, ["r1", "r2"]).median_cv === 0.0
    end

    @testset "Incomplete fold-change designs" begin
        for available in (String[], full_names[1:2], full_names[[1, 3, 4]])
            result, logs = with_metric_warnings() do
                fold_change_metrics_for_table(df, available, design, design.condition_pairs; table_label = "test")
            end
            @test result === nothing
            @test !isempty(logs)
        end
        partial_design = (run_to_condition = merge(mapping, Dict("absent" => "A")),
                          condition_pairs = design.condition_pairs)
        result, logs = with_metric_warnings() do
            fold_change_metrics_for_table(df, full_names, partial_design, partial_design.condition_pairs;
                                          table_label = "test")
        end
        @test result === nothing
        @test any(log -> occursin("missing from table", log.message), logs)

        # A missing condition invalidates only the fold-change pairs that use it.
        extra_pair = (numerator = "C", denominator = "A", expected = Dict("HUMAN" => 1.0))
        independent_design = (run_to_condition = merge(mapping, Dict("absent" => "C")),
                              condition_pairs = [design.condition_pairs; extra_pair])
        result, logs = with_metric_warnings() do
            fold_change_metrics_for_table(df, full_names, independent_design, independent_design.condition_pairs;
                                          table_label = "test")
        end
        @test Set(keys(result)) == Set(["A_over_B"])
        @test result["A_over_B"]["human_median_deviation"] == 1.5
        @test !isempty(logs)

        short_design = (run_to_condition = Dict("A_01" => "A", "A_02" => "A",
                                               "B_01" => "B", "B_02" => "B"),
                        condition_pairs = design.condition_pairs)
        result, logs = with_metric_warnings() do
            fold_change_metrics_for_table(df, full_names, short_design, short_design.condition_pairs;
                                          table_label = "test")
        end
        @test result === nothing
        @test !isempty(logs)
        for value in (missing, NaN, Inf, 0.0)
            invalid = copy(df)
            invalid[!, full_names[1]] = fill(value, 2)
            invalid[!, full_names[2]] = fill(value, 2)
            result, logs = with_metric_warnings() do
                fold_change_metrics_for_table(invalid, full_names, design, design.condition_pairs;
                                              table_label = "test")
            end
            @test result === nothing
            @test any(log -> occursin("no complete, finite observations", log.message), logs)
        end
    end

    @testset "Pipeline serialization" begin
        mktempdir() do root
            for level in ("precursors", "protein_groups")
                Arrow.write(joinpath(root, level * "_wide.arrow"), df)
                Arrow.write(joinpath(root, level * "_long.arrow"), DataFrame(abundance = [1.0]))
            end
            experimental_design = Dict{String, Any}("runs" => mapping)
            for matched in (true, false)
                active_mapping = matched ? mapping : Dict("absent_A" => "A", "absent_B" => "B")
                active_design = (run_to_condition = active_mapping, condition_pairs = design.condition_pairs)
                metrics, logs = with_metric_warnings() do
                    compute_dataset_metrics(root, "test"; metric_groups = ["CV", "fold-change"],
                        experimental_design = Dict{String, Any}("runs" => active_mapping),
                        three_proteome_designs = active_design)
                end
                serialized = JSON.parse(JSON.json(metrics))
                for level in ("precursors", "protein_groups")
                    @test serialized["cv"][level]["median_cv"] === (matched ? 0.0 : nothing)
                end
                if matched
                    @test isempty(logs)
                    @test serialized["fold_change"]["error"]["protein_groups"]["A_over_B"]["human_median_deviation"] == 1.5
                    @test serialized["fold_change"]["variance"]["precursors"]["A_over_B"]["human_fc_variance"] == 0.25
                else
                    @test !isempty(logs)
                    @test !haskey(serialized, "fold_change")
                end
            end
            # Availability is independent for precursor and protein tables.
            Arrow.write(joinpath(root, "protein_groups_wide.arrow"), select(df, Not(full_names[1])))
            metrics, _ = with_metric_warnings() do
                compute_dataset_metrics(root, "test"; metric_groups = ["CV", "fold-change"],
                    experimental_design = experimental_design, three_proteome_designs = design)
            end
            serialized = JSON.parse(JSON.json(metrics))
            @test serialized["cv"]["precursors"]["median_cv"] === 0.0
            @test serialized["cv"]["protein_groups"]["median_cv"] === nothing
            @test haskey(serialized["fold_change"]["error"], "precursors")
            @test !haskey(serialized["fold_change"]["error"], "protein_groups")
        end
    end
end

function configured_three_proteome_tests()
    params_root = joinpath(@__DIR__, "..", "params")
    inventory = JSON.parsefile(joinpath(@__DIR__, "testdata", "three_proteome_input_files.json"))
    configured = String[]
    for dataset in sort(readdir(params_root))
        metrics_path = joinpath(params_root, dataset, "metrics.json")
        isfile(metrics_path) || continue
        preferences = JSON.parsefile(metrics_path; dicttype = Dict)
        any(groups -> any(g -> replace(lowercase(g), "-" => "_") in ("fold_change", "three_proteome"), groups),
            values(preferences)) || continue
        push!(configured, dataset)
    end
    @test Set(configured) == Set(keys(inventory))

    for dataset in configured
        @testset "$dataset" begin
            path = joinpath(params_root, dataset, "experimental_design.json")
            experimental_design = load_experimental_design(path)
            design = load_three_proteome_designs(path)
            groups = run_groups_for_dataset(experimental_design, dataset; three_proteome_designs = design)
            available = first.(splitext.(String.(inventory[dataset])))
            configured_runs = collect(keys(design.run_to_condition))
            @test length(unique(available)) == length(available)
            @test issubset(Set(configured_runs), Set(available))
            @test Set(vcat(values(groups)...)) == Set(configured_runs)
            @test length(vcat(values(groups)...)) == length(configured_runs)
            @test all(length(runs) >= 2 for runs in values(groups))
            conditions, missing_runs = condition_columns(available, design.run_to_condition)
            @test isempty(missing_runs)
            @test Set(keys(conditions)) == Set(experimental_design["condition_keys"])

            for pair in design.condition_pairs
                species = sort(collect(keys(pair.expected)))
                df = DataFrame(species = species, global_qval = fill(0.01, length(species)))
                for run in available
                    condition = get(design.run_to_condition, run, "unconfigured")
                    df[!, run] = condition == pair.numerator ? [10 * pair.expected[s] for s in species] : fill(10.0, length(species))
                end
                result, logs = with_metric_warnings() do
                    cv = compute_cv_metrics(df, available; groups = groups)
                    fc = fold_change_metrics_for_table(df, available, design, [pair]; table_label = dataset)
                    (; cv, fc)
                end
                @test isempty(logs)
                @test result.cv.median_cv ≈ 0.0 atol = 1e-14
                @test result.cv.runs == length(configured_runs)
                label = pair.numerator * "_over_" * pair.denominator
                for s in species
                    @test result.fc[label][lowercase(s) * "_median_deviation"] ≈ 0.0 atol = 1e-14
                    @test result.fc[label][lowercase(s) * "_fc_variance"] == 0.0
                end
            end
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    @testset "Run matching metrics" begin
        run_matching_metrics_tests()
        configured_three_proteome_tests()
    end
end
