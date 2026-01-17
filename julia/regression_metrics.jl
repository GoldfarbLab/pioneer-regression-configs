#!/usr/bin/env julia

using Arrow
using DataFrames
using JSON
using Statistics

include(joinpath(@__DIR__, "metrics", "metrics_helpers.jl"))
include(joinpath(@__DIR__, "metrics", "entrapment_metrics.jl"))
include(joinpath(@__DIR__, "metrics", "three_proteome_metrics.jl"))
include(joinpath(@__DIR__, "metrics", "ftr_metrics.jl"))
include(joinpath(@__DIR__, "metrics", "metrics_pipeline.jl"))
using .RegressionMetricsHelpers: condition_columns, count_nonyeast_ids, count_species_ids, count_total_ids, count_yeast_ids, gene_names_column, mean_for_columns, quant_column_names_from_proteins, select_quant_columns, species_column, unique_species_value
using .EntrapmentMetrics: compute_entrapment_metrics
using .ThreeProteomeMetrics: experimental_design_entry, experimental_design_for_dataset, fold_change_metrics_for_table, gene_counts_metrics_by_run, load_experimental_design, load_three_proteome_designs, normalize_metric_label, run_groups_for_dataset, three_proteome_design_entry

# Default metric buckets used when no per-search overrides are configured.
const DEFAULT_METRIC_GROUPS = ["identification", "CV", "eFDR", "runtime"]

function parse_bool_env(name::AbstractString; default::Bool = false)
    value = get(ENV, name, "")
    if isempty(value)
        return default
    end

    normalized = lowercase(strip(value))
    if normalized in ("1", "true", "yes", "y", "on")
        return true
    elseif normalized in ("0", "false", "no", "n", "off")
        return false
    end

    @warn "Unrecognized boolean environment value; using default" env=name value=value default=default
    default
end

# Load an Arrow table that must exist for metrics computation.
function read_required_table(path::AbstractString)
    isfile(path) || error("Required file not found: $path")
    DataFrame(Arrow.Table(path))
end

# Parse the "Total Runtime" line from a Pioneer report, if present.
function runtime_minutes_from_report(path::AbstractString)
    if !isfile(path)
        @warn "Runtime report not found; skipping runtime metric" path=path
        return nothing
    end

    for line in eachline(path)
        if startswith(line, "Total Runtime:")
            m = match(r"^Total Runtime:\s*([0-9]+(?:\.[0-9]+)?)\s+minutes", line)
            if m !== nothing
                return parse(Float64, m.captures[1])
            end
        end
    end

    @warn "Total runtime line not found in report" path=path
    nothing
end

# Compute run counts and completeness statistics for wide-format quant tables.
function compute_wide_metrics(
    df::DataFrame,
    quant_col_names::AbstractVector{<:Union{Symbol, String}};
    table_label::AbstractString = "wide_table",
    dataset_name::AbstractString = "dataset",
)
    existing_quant_cols = select_quant_columns(df, quant_col_names)
    runs = length(existing_quant_cols)
    available_cols = Symbol.(names(df))

    if runs == 0
        expected = Symbol.(quant_col_names)
        @warn "No quantification columns found in dataset" dataset=dataset_name table_label=table_label expected_quant_cols=expected available_cols=available_cols expected_quant_cols_list=join(expected, ", ") available_cols_list=join(available_cols, ", ")
        return (; runs = 0, complete_rows = 0, data_completeness = 0.0)
    end

    if length(existing_quant_cols) < length(quant_col_names)
        missing_cols = setdiff(Symbol.(quant_col_names), Symbol.(existing_quant_cols))
        expected = Symbol.(quant_col_names)
        available_quant_syms = Symbol.(existing_quant_cols)
        @warn "Missing quantification columns in dataset" dataset=dataset_name table_label=table_label missing_cols=missing_cols expected_quant_cols=expected available_quant_cols=available_quant_syms available_cols=available_cols missing_cols_list=join(missing_cols, ", ") expected_quant_cols_list=join(expected, ", ") available_quant_cols_list=join(available_quant_syms, ", ") available_cols_list=join(available_cols, ", ")
    end

    quant_data = df[:, existing_quant_cols]
    quant_matrix = Matrix(quant_data)

    complete_rows = sum(all(!ismissing, row) for row in eachrow(quant_matrix))
    non_missing_values = count(!ismissing, quant_matrix)
    total_cells = nrow(df) * runs

    (; runs, complete_rows, data_completeness = total_cells > 0 ? non_missing_values / total_cells : 0.0)
end

# Compute median coefficient-of-variation metrics for quant tables.
function compute_cv_metrics(
    df::DataFrame,
    quant_col_names::AbstractVector{<:Union{Symbol, String}};
    table_label::AbstractString = "wide_table",
    groups::Dict{String, Vector{String}} = Dict{String, Vector{String}}(),
)
    function cvs_for_columns(
        columns::AbstractVector{<:Union{Symbol, String}},
        label::AbstractString,
    )
        column_syms = Symbol.(columns)
        runs = length(columns)
        if runs == 0
            return (; runs = 0, rows_evaluated = 0, cvs = Float64[])
        end

        quant_data = df[:, column_syms]
        complete_data = dropmissing(quant_data)
        rows_evaluated = nrow(complete_data)

        first_row_values = rows_evaluated == 0 ? NamedTuple() : NamedTuple(complete_data[1, :])
        @info "Computing CVs for group" table_label=table_label group_label=label quant_columns=column_syms rows_evaluated=rows_evaluated first_row_values=first_row_values
        if rows_evaluated == 0
            return (; runs, rows_evaluated, cvs = Float64[])
        end

        quant_complete = Matrix(complete_data)
        computed_cvs = Float64[]
        for row in eachrow(quant_complete)
            mean_val = mean(row)
            if mean_val != 0
                push!(computed_cvs, std(row) / mean_val)
            end
        end

        (; runs, rows_evaluated, cvs = computed_cvs)
    end

    if isempty(groups)
        existing_quant_cols = select_quant_columns(df, quant_col_names)
        stats = cvs_for_columns(existing_quant_cols, "all_runs")
        median_cv = isempty(stats.cvs) ? 0.0 : median(stats.cvs)
        return (; runs = stats.runs, rows_evaluated = stats.rows_evaluated, median_cv)
    end

    all_runs = Set{Symbol}()
    all_cvs = Float64[]
    total_rows = 0
    for (label, runs) in pairs(groups)
        columns = select_quant_columns(df, runs)
        union!(all_runs, Symbol.(columns))
        stats = cvs_for_columns(columns, label)
        total_rows += stats.rows_evaluated
        append!(all_cvs, stats.cvs)
    end

    median_cv = isempty(all_cvs) ? 0.0 : median(all_cvs)
    (; runs = length(all_runs), rows_evaluated = total_rows, median_cv)
end

# Normalize metric group entries from JSON config to string arrays.
function normalize_metric_groups(groups)
    if groups isa AbstractVector
        return [String(g) for g in groups]
    end

    @warn "Invalid metric groups entry; falling back to empty list" groups_type=typeof(groups)
    String[]
end

# Resolve metric groups for a dataset, honoring defaults or per-dataset overrides.
function metric_preferences(config::Dict, dataset_name::AbstractString)
    entry = get(config, dataset_name, DEFAULT_METRIC_GROUPS)

    if entry isa AbstractDict
        groups = normalize_metric_groups(get(entry, "groups", DEFAULT_METRIC_GROUPS))
        return (; groups)
    end

    groups = normalize_metric_groups(entry)
    (; groups)
end

# Resolve metric groups for a search, supporting list or keyed config entries.
function metric_groups_for_search(config::Dict, search_name::AbstractString)
    entry = get(config, search_name, DEFAULT_METRIC_GROUPS)

    if entry isa AbstractDict
        groups = get(entry, "groups", DEFAULT_METRIC_GROUPS)
        return normalize_metric_groups(groups)
    elseif entry isa AbstractVector
        return normalize_metric_groups(entry)
    else
        @warn "Invalid metrics entry for search; using defaults" search=search_name entry_type=typeof(entry)
        return DEFAULT_METRIC_GROUPS
    end
end


# Collect dataset directories under a regression results root.
function dataset_dirs_from_root(root::AbstractString)
    !isdir(root) && return String[]
    filter(readdir(root; join=true)) do path
        isdir(path)
    end
end

# Resolve the regression results root from env/args.
function resolve_results_root()
    run_dir = get(ENV, "RUN_DIR", "")
    arg_root = length(ARGS) >= 1 ? ARGS[1] : ""

    if !isempty(arg_root)
        return arg_root
    elseif !isempty(run_dir)
        return joinpath(run_dir, "results")
    end

    error("Results directory is not specified; set RUN_DIR or pass a path argument")
end

# Load a JSON config that customizes which metric groups run per dataset/search.
function load_metrics_config(path::AbstractString)
    if isempty(path)
        @info "No metrics config provided; using defaults for all searches"
        return Dict{String, Any}()
    end

    if !isfile(path)
        @warn "Metrics config not found; using defaults for all searches" metrics_config_path=path
        return Dict{String, Any}()
    end

    try
        raw = JSON.parsefile(path; dicttype=Dict)
        if raw isa AbstractDict
            return Dict{String, Any}(String(k) => v for (k, v) in raw)
        end
        @warn "Metrics config is not a dictionary; using defaults" metrics_config_path=path
        return Dict{String, Any}()
    catch err
        @warn "Failed to parse metrics config; using defaults" metrics_config_path=path error=err
        return Dict{String, Any}()
    end
end

# Find all search parameter JSON files under a params directory.
function search_param_files(params_dir::AbstractString)
    !isdir(params_dir) && return String[]
    filter(readdir(params_dir; join=true)) do path
        isfile(path) && occursin(r"search.*\.json$", basename(path))
    end
end

function results_dir_from_param(path::AbstractString)
    try
        parsed = JSON.parsefile(path)
        results = get(parsed, "results", nothing)
        if results isa AbstractString
            return results
        end

        paths_block = get(parsed, "paths", nothing)
        if paths_block isa AbstractDict
            nested_results = get(paths_block, "results", nothing)
            if nested_results isa AbstractString
                return nested_results
            end
        end
    catch err
        @warn "Failed to parse search param file" param_path=path error=err
        return nothing
    end

    @warn "Search param file missing results entry" param_path=path
    nothing
end

function dataset_name_from_results_dir(results_dir::AbstractString)
    normalized = normpath(results_dir)
    parts = filter(!isempty, split(normalized, '/'))
    if length(parts) >= 3
        return parts[end - 1]
    elseif !isempty(parts)
        return parts[end]
    end
    basename(normalized)
end

# Metrics files follow metrics_<dataset>_<search>.json; reports derive search from filename suffix.
function output_metrics_path(results_dir::AbstractString, dataset_name::AbstractString, search_name::AbstractString)
    filename = string("metrics_", dataset_name, "_", search_name, ".json")
    joinpath(results_dir, filename)
end

function is_log_file(path::AbstractString)
    lowercasepath = lowercase(path)
    endswith(lowercasepath, ".log")
end

function cleanup_entrapment_dir(entrapment_dir::AbstractString)
    isdir(entrapment_dir) || return

    for entry in readdir(entrapment_dir; join = true)
        if isfile(entry)
            endswith(entry, ".png") && continue
            is_log_file(entry) && continue
            safe_rm(entry; force = true, recursive = true)
        elseif isdir(entry) && basename(entry) == "qc_plots"
            continue
        else
            safe_rm(entry; force = true, recursive = true)
        end
    end
end

function safe_rm(path::AbstractString; force::Bool = false, recursive::Bool = false)
    try
        rm(path; force = force, recursive = recursive)
    catch err
        @warn "Failed to remove path" path=path error=err
    end
end

function safe_mv(src::AbstractString, dst::AbstractString; force::Bool = false)
    try
        mv(src, dst; force = force)
    catch err
        @warn "Failed to move path" src=src dst=dst error=err
    end
end

function cleanup_results_dir(results_dir::AbstractString, metrics_path::AbstractString)
    if !isdir(results_dir)
        @warn "Results directory missing during cleanup" results_dir = results_dir
        return
    end

    metrics_path_abs = abspath(metrics_path)

    for entry in readdir(results_dir; join = true)
        entry_abs = abspath(entry)
        if entry_abs == metrics_path_abs
            continue
        end

        if isfile(entry)
            if is_log_file(entry)
                continue
            end
        elseif isdir(entry) && basename(entry) == "entrapment_analysis"
            cleanup_entrapment_dir(entry)
            continue
        elseif isdir(entry) && basename(entry) == "qc_plots"
            continue
        end

        safe_rm(entry; force = true, recursive = true)
    end
end

function archive_results(
    results_dir::AbstractString,
    metrics_path::AbstractString;
    archive_root::AbstractString = "",
    dataset_name::AbstractString = "dataset",
    search_name::AbstractString = "search",
    preserve_results::Bool = false,
)
    if isempty(archive_root)
        @info "Archive root not provided; skipping move of regression outputs" results_dir=results_dir
        return
    end

    dataset_target_dir = joinpath(archive_root, "results", dataset_name)
    search_target_dir = joinpath(dataset_target_dir, search_name)
    @info "Resolved archive destination for results" results_dir=results_dir dataset_name=dataset_name search_name=search_name target_dir=search_target_dir
    mkpath(search_target_dir)

    if preserve_results
        for entry in readdir(results_dir; join = true)
            if isfile(entry) && abspath(entry) == abspath(metrics_path)
                safe_mv(entry, joinpath(dataset_target_dir, basename(entry)); force = true)
            else
                safe_mv(entry, joinpath(search_target_dir, basename(entry)); force = true)
            end
        end
        @info "Archived regression outputs (preserved original results)" results_dir=results_dir metrics_path=metrics_path target_dir=search_target_dir
        return
    end

    if isfile(metrics_path)
        target_metrics = joinpath(dataset_target_dir, basename(metrics_path))
        safe_mv(metrics_path, target_metrics; force = true)
        metrics_path = target_metrics
    end

    entrapment_dir = joinpath(results_dir, "entrapment_analysis")
    if isdir(entrapment_dir)
        for entry in readdir(entrapment_dir; join = true)
            if isfile(entry) && endswith(lowercase(entry), ".png")
                safe_mv(entry, joinpath(search_target_dir, basename(entry)); force = true)
            end
        end
    end

    qc_plots_dir = joinpath(results_dir, "qc_plots")
    if isdir(qc_plots_dir)
        safe_mv(qc_plots_dir, joinpath(search_target_dir, "qc_plots"); force = true)
    end

    for entry in readdir(results_dir; join = true)
        if isfile(entry) && is_log_file(entry)
            safe_mv(entry, joinpath(search_target_dir, basename(entry)); force = true)
        end
    end

    safe_rm(results_dir; force = true, recursive = true)

    @info "Archived regression outputs" results_dir=results_dir metrics_path=metrics_path target_dir=search_target_dir
end

function compute_metrics_for_params_dir(
    params_dir::AbstractString;
    metrics_config_path::AbstractString = "",
    experimental_design_path::AbstractString = "",
    three_proteome_designs_path::AbstractString = "",
    archive_root::AbstractString = "",
    preserve_results::Bool = false,
)
    isdir(params_dir) || error("Params directory does not exist: $params_dir")

    param_files = search_param_files(params_dir)
    isempty(param_files) && error("No search*.json files found in params directory $params_dir")

    metrics_config = load_metrics_config(metrics_config_path)
    experimental_design = isempty(experimental_design_path) ? Dict{String, Any}() : load_experimental_design(experimental_design_path)

    dataset_entries = NamedTuple{(:search_name, :results_dir, :dataset_name)}[]
    dataset_paths = Dict{String, String}()

    for param_file in param_files
        results_dir = results_dir_from_param(param_file)
        results_dir === nothing && error("Missing results entry in search param file $param_file")

        search_name = replace(basename(param_file), ".json" => "")
        dataset_name = dataset_name_from_results_dir(results_dir)
        push!(dataset_entries, (; search_name, results_dir, dataset_name))
        haskey(dataset_paths, dataset_name) || (dataset_paths[dataset_name] = results_dir)
    end

    isempty(dataset_entries) && error("No valid parameter files remain after parsing results paths")

    three_proteome_designs = nothing

    for entry in dataset_entries
        metric_groups = metric_groups_for_search(metrics_config, entry.search_name)
        normalized_groups = Set(replace.(lowercase.(metric_groups), "-" => "_"))
        need_three_proteome =
            ("fold_change" in normalized_groups) ||
            ("three_proteome" in normalized_groups)

        if need_three_proteome && three_proteome_designs === nothing && !isempty(three_proteome_designs_path)
            three_proteome_designs = load_three_proteome_designs(three_proteome_designs_path)
        end

        metrics = compute_dataset_metrics(
            entry.results_dir,
            entry.dataset_name;
            metric_groups = metric_groups,
            experimental_design = experimental_design,
            three_proteome_designs = three_proteome_designs,
            dataset_paths = dataset_paths,
        )

        output_path = output_metrics_path(entry.results_dir, entry.dataset_name, entry.search_name)

        metrics === nothing && begin
            @warn "No metrics produced for search; archiving logs only" search=entry.search_name dataset=entry.dataset_name results_dir=entry.results_dir
            preserve_results || cleanup_results_dir(entry.results_dir, output_path)
            archive_results(
                entry.results_dir,
                output_path;
                archive_root = archive_root,
                dataset_name = entry.dataset_name,
                search_name = entry.search_name,
                preserve_results = preserve_results,
            )
            continue
        end

        if metrics isa AbstractDict && isempty(metrics)
            @warn "No metrics produced for search; archiving logs only" search=entry.search_name dataset=entry.dataset_name results_dir=entry.results_dir
            preserve_results || cleanup_results_dir(entry.results_dir, output_path)
            archive_results(
                entry.results_dir,
                output_path;
                archive_root = archive_root,
                dataset_name = entry.dataset_name,
                search_name = entry.search_name,
                preserve_results = preserve_results,
            )
            continue
        end

        open(output_path, "w") do io
            JSON.print(io, metrics)
        end

        preserve_results || cleanup_results_dir(entry.results_dir, output_path)
        archive_results(
            entry.results_dir,
            output_path;
            archive_root = archive_root,
            dataset_name = entry.dataset_name,
            search_name = entry.search_name,
            preserve_results = preserve_results,
        )
    end
end

function main()
    run_dir = get(ENV, "RUN_DIR", "")
    dataset_name = get(ENV, "PIONEER_DATASET_NAME", "")
    delete_results = parse_bool_env("PIONEER_DELETE_RESULTS")
    preserve_results = !delete_results
    isempty(run_dir) && error("RUN_DIR must be set for regression metrics")
    archive_root = run_dir

    if !isempty(dataset_name)
        params_dir = joinpath(run_dir, "adjusted-params", dataset_name)
        config_dir = joinpath(run_dir, "regression-configs", "params", dataset_name)
        metrics_config_path = joinpath(config_dir, "metrics.json")
        experimental_design_path = config_dir
        three_proteome_designs_path = experimental_design_path
        compute_metrics_for_params_dir(
            params_dir;
            metrics_config_path = metrics_config_path,
            experimental_design_path = experimental_design_path,
            three_proteome_designs_path = three_proteome_designs_path,
            archive_root = archive_root,
            preserve_results = preserve_results,
        )
        return
    end

    results_dir = resolve_results_root()
    isdir(results_dir) || error("Results directory does not exist: $results_dir")
    dataset_dirs = dataset_dirs_from_root(results_dir)
    isempty(dataset_dirs) && error("No dataset directories found in $results_dir")

    dataset_paths = Dict{String, String}(basename(path) => path for path in dataset_dirs)

    @info "Using regression results directory" results_dir=results_dir

    repo_root = joinpath(run_dir, "regression-configs")
    metrics_config_path = joinpath(repo_root, "metrics_config.json")
    metric_group_config = if isfile(metrics_config_path)
        JSON.parsefile(metrics_config_path; dicttype=Dict)
    else
        @info "No metrics_config.json found; using default metric groups" metrics_config_path=metrics_config_path
        Dict{String, Any}()
    end

    experimental_design_path = joinpath(repo_root, "experimental_designs")
    experimental_design = load_experimental_design(experimental_design_path)

    three_proteome_designs_path = joinpath(repo_root, "experimental_designs")
    three_proteome_designs = nothing

    dataset_dirs = filter(dataset_dirs) do path
        dataset_name = basename(path)
        preferences = metric_preferences(metric_group_config, dataset_name)
        if isempty(preferences.groups)
            @info "Skipping dataset without requested metrics" dataset=dataset_name
            return false
        end
        return true
    end

    isempty(dataset_dirs) && error("No dataset directories remain after filtering")

    for dataset_dir in dataset_dirs
        dataset_name = basename(dataset_dir)

        preferences = metric_preferences(metric_group_config, dataset_name)

        metric_groups = preferences.groups
        normalized_groups = Set(replace.(lowercase.(metric_groups), "-" => "_"))
        need_three_proteome =
            ("fold_change" in normalized_groups) ||
            ("three_proteome" in normalized_groups)
        if need_three_proteome && three_proteome_designs === nothing
            three_proteome_designs = load_three_proteome_designs(three_proteome_designs_path)
        end

        metrics = compute_dataset_metrics(
            dataset_dir,
            dataset_name;
            metric_groups = metric_groups,
            experimental_design = experimental_design,
            three_proteome_designs = three_proteome_designs,
            dataset_paths = dataset_paths,
        )
        metrics === nothing && continue

        output_path = joinpath(dataset_dir, "metrics_$(dataset_name).json")
        open(output_path, "w") do io
            JSON.print(io, metrics)
        end
    end
end

main()
