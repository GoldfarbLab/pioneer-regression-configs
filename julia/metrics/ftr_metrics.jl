using DataFrames

function ftr_metrics_for_table(
    combined_df::DataFrame,
    human_only_df::DataFrame,
    human_yeast_df::DataFrame,
    combined_quant_cols::AbstractVector{<:Union{Symbol, String}},
    human_only_quant_cols::AbstractVector{<:Union{Symbol, String}},
    human_yeast_quant_cols::AbstractVector{<:Union{Symbol, String}},
    human_only_runs::AbstractVector{<:AbstractString};
    table_label::AbstractString,
)
    combined_total_ids = count_total_ids(combined_df, combined_quant_cols, nothing; table_label = table_label)
    split_human_only_total_ids = count_total_ids(human_only_df, human_only_quant_cols, nothing; table_label = table_label)
    split_human_yeast_total_ids = count_total_ids(human_yeast_df, human_yeast_quant_cols, nothing; table_label = table_label)
    split_total_ids = split_human_only_total_ids + split_human_yeast_total_ids

    combined_human_only_yeast_ids = count_yeast_ids(
        combined_df,
        combined_quant_cols,
        human_only_runs;
        table_label = table_label,
    )
    split_human_only_yeast_ids = count_yeast_ids(
        human_only_df,
        human_only_quant_cols,
        nothing;
        table_label = table_label,
    )

    additional_yeast_IDs = combined_human_only_yeast_ids - split_human_only_yeast_ids
    additional_IDs = combined_total_ids - split_total_ids
    ftr = additional_yeast_IDs < 0 || additional_IDs < 0 ? 0.0 :
        additional_IDs > 0 ? additional_yeast_IDs / additional_IDs : 0.0

    return Dict(
        "additional_yeast_IDs" => additional_yeast_IDs,
        "additional_IDs" => additional_IDs,
        "false_transfer_rate" => ftr,
    )
end

function false_transfer_rate_config(entry)
    if entry isa AbstractDict
        ftr_config = get(entry, "false_transfer_rate", nothing)
        if ftr_config isa AbstractDict
            return Dict{String, Any}(String(k) => v for (k, v) in ftr_config)
        end
    end

    Dict{String, Any}()
end

function config_string(
    config::AbstractDict,
    key::AbstractString,
    default::Union{Nothing, AbstractString} = nothing,
)
    value = get(config, key, default)
    value isa AbstractString && return String(value)

    default === nothing && return nothing
    return String(default)
end

function human_only_condition_from_design(entry)
    if entry isa AbstractDict
        ftr_config = false_transfer_rate_config(entry)
        condition = config_string(ftr_config, "human_only_condition")
        condition !== nothing && return condition

        condition = get(entry, "human_only_condition", nothing)
        condition isa AbstractString && return String(condition)
    end

    return "human_only"
end

function table_for_search(
    search_name::AbstractString,
    current_search_name::AbstractString,
    current_df::DataFrame,
    search_paths::Dict{String, String},
    filename::AbstractString;
    table_label::AbstractString,
)
    if search_name == current_search_name
        return current_df
    end

    search_path = get(search_paths, search_name, nothing)
    search_path === nothing && begin
        @warn "Missing configured FTR comparison search; skipping" search=search_name table=table_label available_searches=sort(collect(keys(search_paths)))
        return nothing
    end

    arrow_path = joinpath(search_path, filename)
    isfile(arrow_path) || begin
        @warn "Missing FTR comparison table; skipping" search=search_name table=table_label path=arrow_path
        return nothing
    end

    read_required_table(arrow_path)
end

function compute_ftr_metrics(
    dataset_name::AbstractString,
    search_name::AbstractString,
    precursors_wide::DataFrame,
    protein_groups_wide::DataFrame,
    experimental_design::Dict{String, Any},
    search_paths::Dict{String, String},
)
    entry = experimental_design_entry(experimental_design, dataset_name)
    ftr_config = false_transfer_rate_config(entry)
    combined_search = config_string(ftr_config, "combined_search")
    human_only_search = config_string(ftr_config, "human_only_search")
    human_yeast_search = config_string(ftr_config, "human_yeast_search")

    if combined_search === nothing || human_only_search === nothing || human_yeast_search === nothing
        @warn "FTR metrics require configured combined and split search names" dataset=dataset_name search=search_name config=ftr_config
        return nothing
    end

    if search_name != combined_search
        @info "Skipping FTR metrics for non-combined search" dataset=dataset_name search=search_name combined_search=combined_search
        return nothing
    end

    groups = run_groups_for_dataset(experimental_design, dataset_name)
    human_only_condition = human_only_condition_from_design(entry)
    human_only_runs = get(groups, human_only_condition, String[])
    if isempty(human_only_runs) && human_only_condition != "human_only"
        human_only_runs = get(groups, "human_only", String[])
    end

    if isempty(human_only_runs)
        @warn "No human-only runs provided for FTR metrics; skipping" dataset=dataset_name expected_condition=human_only_condition
        return nothing
    end

    precursors_combined = table_for_search(
        combined_search,
        search_name,
        precursors_wide,
        search_paths,
        "precursors_wide.arrow";
        table_label = "precursors",
    )
    precursors_combined === nothing && return nothing
    precursors_human_only = table_for_search(
        human_only_search,
        search_name,
        precursors_wide,
        search_paths,
        "precursors_wide.arrow";
        table_label = "precursors",
    )
    precursors_human_only === nothing && return nothing
    precursors_human_yeast = table_for_search(
        human_yeast_search,
        search_name,
        precursors_wide,
        search_paths,
        "precursors_wide.arrow";
        table_label = "precursors",
    )
    precursors_human_yeast === nothing && return nothing

    protein_groups_combined = table_for_search(
        combined_search,
        search_name,
        protein_groups_wide,
        search_paths,
        "protein_groups_wide.arrow";
        table_label = "protein_groups",
    )
    protein_groups_combined === nothing && return nothing
    protein_groups_human_only = table_for_search(
        human_only_search,
        search_name,
        protein_groups_wide,
        search_paths,
        "protein_groups_wide.arrow";
        table_label = "protein_groups",
    )
    protein_groups_human_only === nothing && return nothing
    protein_groups_human_yeast = table_for_search(
        human_yeast_search,
        search_name,
        protein_groups_wide,
        search_paths,
        "protein_groups_wide.arrow";
        table_label = "protein_groups",
    )
    protein_groups_human_yeast === nothing && return nothing

    combined_quant_cols = quant_column_names_from_proteins(precursors_combined)
    human_only_quant_cols = quant_column_names_from_proteins(precursors_human_only)
    human_yeast_quant_cols = quant_column_names_from_proteins(precursors_human_yeast)

    protein_combined_quant_cols = quant_column_names_from_proteins(protein_groups_combined)
    protein_human_only_quant_cols = quant_column_names_from_proteins(protein_groups_human_only)
    protein_human_yeast_quant_cols = quant_column_names_from_proteins(protein_groups_human_yeast)

    precursor_metrics = ftr_metrics_for_table(
        precursors_combined,
        precursors_human_only,
        precursors_human_yeast,
        combined_quant_cols,
        human_only_quant_cols,
        human_yeast_quant_cols,
        human_only_runs;
        table_label = "precursors",
    )

    protein_metrics = ftr_metrics_for_table(
        protein_groups_combined,
        protein_groups_human_only,
        protein_groups_human_yeast,
        protein_combined_quant_cols,
        protein_human_only_quant_cols,
        protein_human_yeast_quant_cols,
        human_only_runs;
        table_label = "protein_groups",
    )

    return Dict(
        "precursors" => precursor_metrics,
        "protein_groups" => protein_metrics,
    )
end
