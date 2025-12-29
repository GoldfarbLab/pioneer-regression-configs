#!/usr/bin/env julia

using JSON
using Printf

const NA = ""

function parse_version_label(label::AbstractString)
    stripped = startswith(label, "v") ? label[2:end] : label
    parts = split(stripped, '.')
    if length(parts) != 3
        return nothing
    end
    nums = Int[]
    for part in parts
        m = match(r"^\d+$", part)
        m === nothing && return nothing
        push!(nums, parse(Int, part))
    end
    nums
end

function sorted_release_versions(root::AbstractString)
    if !isdir(root)
        return String[]
    end
    versions = filter(readdir(root; join=true)) do path
        isdir(path)
    end
    labels = map(basename, versions)
    sort(labels; lt = (a, b) -> begin
        parsed_a = parse_version_label(a)
        parsed_b = parse_version_label(b)
        if parsed_a === nothing || parsed_b === nothing
            return a < b
        end
        return parsed_a < parsed_b
    end)
end

function flatten_metrics(metrics::Any; prefix::AbstractString = "")
    flat = Dict{String, Float64}()
    if metrics isa AbstractDict
        for (key, value) in pairs(metrics)
            key_str = String(key)
            next_prefix = isempty(prefix) ? key_str : string(prefix, ".", key_str)
            child = flatten_metrics(value; prefix = next_prefix)
            merge!(flat, child)
        end
        return flat
    end
    if metrics isa Number
        flat[prefix] = Float64(metrics)
    end
    flat
end

function metrics_files(root::AbstractString)
    files = String[]
    if !isdir(root)
        return files
    end
    for (dir, _, filenames) in walkdir(root)
        for filename in filenames
            if startswith(filename, "metrics_") && endswith(filename, ".json")
                push!(files, joinpath(dir, filename))
            end
        end
    end
    files
end

function parse_dataset_search(path::AbstractString, root::AbstractString)
    rel_path = relpath(path, root)
    parts = splitpath(rel_path)
    search = length(parts) >= 2 ? parts[1] : ""
    base = replace(basename(path), r"^metrics_" => "", r"\.json$" => "")
    if isempty(search) && occursin("_", base)
        chunks = split(base, "_")
        if length(chunks) >= 2
            search = chunks[end]
            dataset = join(chunks[1:end-1], "_")
            return dataset, search
        end
    end
    dataset = base
    if !isempty(search)
        suffix = "_" * search
        if endswith(dataset, suffix)
            dataset = dataset[1:end - length(suffix)]
        end
    end
    dataset, search
end

function collect_metrics(root::AbstractString)
    results = Dict{String, Dict{String, Dict{String, Float64}}}()
    for path in metrics_files(root)
        dataset, search = parse_dataset_search(path, root)
        if isempty(search)
            search = "unknown-search"
        end
        metrics_json = JSON.parsefile(path; dicttype = Dict)
        flat = flatten_metrics(metrics_json)
        search_entry = get!(results, search, Dict{String, Dict{String, Float64}}())
        search_entry[dataset] = flat
    end
    results
end

function format_number(value::Float64)
    if value == floor(value)
        return string(Int(value))
    end
    return @sprintf("%.4f", value)
end

function format_value(value)
    if value === missing || value === nothing
        return NA
    elseif value isa Float64
        return format_number(value)
    elseif value isa Number
        return format_number(Float64(value))
    end
    NA
end

function format_delta(current, previous)
    if current === missing || previous === missing || current === nothing || previous === nothing
        return NA
    elseif current isa Number && previous isa Number
        delta = Float64(current) - Float64(previous)
        formatted = format_number(abs(delta))
        return delta >= 0 ? "+" * formatted : "-" * formatted
    end
    NA
end

function format_percent_delta(current, previous)
    if current === missing || previous === missing || current === nothing || previous === nothing
        return NA
    elseif current isa Number && previous isa Number
        prev_value = Float64(previous)
        prev_value == 0 && return NA
        percent = (Float64(current) - prev_value) / prev_value * 100
        formatted = format_number(abs(percent)) * "%"
        return percent >= 0 ? "+" * formatted : "-" * formatted
    end
    NA
end

function metric_group(metric::AbstractString)
    parts = split(metric, ".")
    isempty(parts) ? metric : parts[1]
end

function parse_fold_change_metric(metric::AbstractString)
    parts = split(metric, ".")
    length(parts) < 5 && return nothing
    parts[1] == "fold_change" || return nothing
    parts[2] == "error" || return nothing
    group = parts[3]
    condition = parts[4]
    species_metric = join(parts[5:end], ".")
    suffix = "_median_deviation"
    endswith(species_metric, suffix) || return nothing
    species = species_metric[1:end - length(suffix)]
    (group, condition, species)
end

function parse_fold_change_fc_variance_metric(metric::AbstractString)
    parts = split(metric, ".")
    length(parts) < 5 && return nothing
    parts[1] == "fold_change" || return nothing
    parts[2] == "variance" || return nothing
    group = parts[3]
    condition = parts[4]
    species_metric = join(parts[5:end], ".")
    suffix = "_fc_variance"
    endswith(species_metric, suffix) || return nothing
    species = species_metric[1:end - length(suffix)]
    (group, condition, species)
end

function parse_keap1_metric(metric::AbstractString)
    parts = split(metric, ".")
    length(parts) < 3 && return nothing
    parts[1] == "keap1" || return nothing
    group = parts[2]
    label = parts[3]
    split_label = split(label, "_"; limit = 2)
    gene = split_label[1]
    run = length(split_label) == 2 ? split_label[2] : ""
    (group, gene, run)
end

format_species_label(species::AbstractString) = replace(species, "_" => " ")

function format_condition_label(condition::AbstractString)
    label = replace(condition, "_over_" => " vs ")
    replace(label, "_" => " ")
end

function normalized_group(group::AbstractString)
    replace(group, "-" => "_")
end

function metric_group_order()
    Dict(
        "identification" => 1,
        "entrapment" => 2,
        "ftr" => 3,
        "keap1" => 4,
        "cv" => 5,
        "fold_change" => 6,
        "runtime" => 8,
    )
end

function ordered_metrics(metrics::Vector{String})
    order_map = metric_group_order()
    sort(metrics; by = metric -> begin
        group = normalized_group(metric_group(metric))
        (get(order_map, group, length(order_map) + 1), metric)
    end)
end

function include_metric(metric::AbstractString)
    if endswith(metric, ".complete_rows") || endswith(metric, ".data_completeness")
        return false
    end

    if startswith(metric, "entrapment.")
        return endswith(metric, ".difference")
    end

    if startswith(metric, "ftr.")
        return endswith(metric, ".false_transfer_rate") &&
            (occursin(".precursors.", metric) || occursin(".protein_groups.", metric))
    end

    if parse_fold_change_fc_variance_metric(metric) !== nothing
        return false
    end

    return true
end

function collect_fold_change_entries(version_data::AbstractDict{String, Any}, versions::Vector{String})
    entries = Dict{String, Set{Tuple{String, String, String, String}}}()
    for version in versions
        searches_entry = get(version_data, version, Dict{String, Any}())
        searches_entry isa AbstractDict || continue
        for (search, datasets) in pairs(searches_entry)
            datasets isa AbstractDict || continue
            for (dataset, metrics) in pairs(datasets)
                metrics isa AbstractDict || continue
                for metric in keys(metrics)
                    parsed = parse_fold_change_metric(String(metric))
                    parsed === nothing && continue
                    group, condition, species = parsed
                    entry_set = get!(entries, group, Set{Tuple{String, String, String, String}}())
                    push!(entry_set, (String(search), String(dataset), species, condition))
                end
            end
        end
    end
    entries
end

function collect_fold_change_fc_variance_entries(
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    entries = Dict{String, Set{Tuple{String, String, String, String}}}()
    for version in versions
        searches_entry = get(version_data, version, Dict{String, Any}())
        searches_entry isa AbstractDict || continue
        for (search, datasets) in pairs(searches_entry)
            datasets isa AbstractDict || continue
            for (dataset, metrics) in pairs(datasets)
                metrics isa AbstractDict || continue
                for metric in keys(metrics)
                    parsed = parse_fold_change_fc_variance_metric(String(metric))
                    parsed === nothing && continue
                    group, condition, species = parsed
                    entry_set = get!(entries, group, Set{Tuple{String, String, String, String}}())
                    push!(entry_set, (String(search), String(dataset), species, condition))
                end
            end
        end
    end
    entries
end

function collect_keap1_entries(version_data::AbstractDict{String, Any}, versions::Vector{String})
    entries = Dict{String, Set{Tuple{String, String, String, String, String}}}()
    for version in versions
        searches_entry = get(version_data, version, Dict{String, Any}())
        searches_entry isa AbstractDict || continue
        for (search, datasets) in pairs(searches_entry)
            datasets isa AbstractDict || continue
            for (dataset, metrics) in pairs(datasets)
                metrics isa AbstractDict || continue
                for metric in keys(metrics)
                    parsed = parse_keap1_metric(String(metric))
                    parsed === nothing && continue
                    group, gene, run = parsed
                    entry_set = get!(
                        entries,
                        group,
                        Set{Tuple{String, String, String, String, String}}(),
                    )
                    push!(entry_set, (String(search), String(dataset), gene, run, String(metric)))
                end
            end
        end
    end
    entries
end

function write_fold_change_table(
    buffer::IOBuffer,
    group::AbstractString,
    entries::Set{Tuple{String, String, String, String}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    isempty(entries) && return
    println(buffer, "fold_change.error.", group)
    println(buffer, "")

    header_parts = vcat(["Search", "Species", "Condition"], versions)
    if length(versions) > 1
        append!(
            header_parts,
            ["Δ " * versions[idx] * " vs prev" for idx in 2:length(versions)],
        )
    end
    println(buffer, "| ", join(header_parts, " | "), " |")
    println(buffer, "| ", join(fill("---", length(header_parts)), " | "), " |")

    entries_sorted = sort(collect(entries); by = entry -> (entry[1], entry[2], entry[3], entry[4]))
    for (search, dataset, species, condition) in entries_sorted
        values = Vector{Any}(undef, length(versions))
        metric_name = string(
            "fold_change.error.",
            group,
            ".",
            condition,
            ".",
            species,
            "_median_deviation",
        )
        for (idx, version) in enumerate(versions)
            searches_entry = get(version_data, version, Dict{String, Any}())
            metrics_by_search = searches_entry isa AbstractDict ?
                get(searches_entry, search, Dict{String, Any}()) :
                Dict{String, Any}()
            metrics = metrics_by_search isa AbstractDict ?
                get(metrics_by_search, dataset, Dict{String, Any}()) :
                Dict{String, Any}()
            values[idx] = metrics isa AbstractDict ? get(metrics, metric_name, missing) : missing
        end

        has_value = any(value -> !(value === missing || value === nothing), values)
        has_value || continue

        deltas = String[]
        if length(values) > 1
            prev_value = values[1]
            for idx in 2:length(values)
                push!(deltas, format_delta(values[idx], prev_value))
                prev_value = values[idx]
            end
        end

        row_parts = vcat(
            [search, format_species_label(species), format_condition_label(condition)],
            [format_value(value) for value in values],
            deltas,
        )
        println(buffer, "| ", join(row_parts, " | "), " |")
    end
    println(buffer, "")
end

function write_fold_change_tables(
    buffer::IOBuffer,
    fold_change_entries::Dict{String, Set{Tuple{String, String, String, String}}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    for group in ("precursors", "protein_groups")
        entries = get(fold_change_entries, group, Set{Tuple{String, String, String, String}}())
        write_fold_change_table(buffer, group, entries, version_data, versions)
    end
end

function write_fold_change_fc_variance_table(
    buffer::IOBuffer,
    group::AbstractString,
    entries::Set{Tuple{String, String, String, String}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    isempty(entries) && return
    println(buffer, "fold_change.variance.", group)
    println(buffer, "")

    header_parts = vcat(["Search", "Species", "Condition"], versions)
    if length(versions) > 1
        append!(
            header_parts,
            ["Δ " * versions[idx] * " vs prev" for idx in 2:length(versions)],
        )
    end
    println(buffer, "| ", join(header_parts, " | "), " |")
    println(buffer, "| ", join(fill("---", length(header_parts)), " | "), " |")

    entries_sorted = sort(collect(entries); by = entry -> (entry[1], entry[2], entry[3], entry[4]))
    for (search, dataset, species, condition) in entries_sorted
        values = Vector{Any}(undef, length(versions))
        metric_name = string(
            "fold_change.variance.",
            group,
            ".",
            condition,
            ".",
            species,
            "_fc_variance",
        )
        for (idx, version) in enumerate(versions)
            searches_entry = get(version_data, version, Dict{String, Any}())
            metrics_by_search = searches_entry isa AbstractDict ?
                get(searches_entry, search, Dict{String, Any}()) :
                Dict{String, Any}()
            metrics = metrics_by_search isa AbstractDict ?
                get(metrics_by_search, dataset, Dict{String, Any}()) :
                Dict{String, Any}()
            values[idx] = metrics isa AbstractDict ? get(metrics, metric_name, missing) : missing
        end

        has_value = any(value -> !(value === missing || value === nothing), values)
        has_value || continue

        deltas = String[]
        if length(values) > 1
            prev_value = values[1]
            for idx in 2:length(values)
                push!(deltas, format_delta(values[idx], prev_value))
                prev_value = values[idx]
            end
        end

        row_parts = vcat(
            [search, format_species_label(species), format_condition_label(condition)],
            [format_value(value) for value in values],
            deltas,
        )
        println(buffer, "| ", join(row_parts, " | "), " |")
    end
    println(buffer, "")
end

function write_fold_change_fc_variance_tables(
    buffer::IOBuffer,
    fold_change_fc_variance_entries::Dict{String, Set{Tuple{String, String, String, String}}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    for group in ("precursors", "protein_groups")
        entries = get(fold_change_fc_variance_entries, group, Set{Tuple{String, String, String, String}}())
        write_fold_change_fc_variance_table(buffer, group, entries, version_data, versions)
    end
end

function write_keap1_table(
    buffer::IOBuffer,
    group::AbstractString,
    entries::Set{Tuple{String, String, String, String, String}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    isempty(entries) && return
    println(buffer, "keap1.", group)
    println(buffer, "")

    header_parts = vcat(["Search", "Gene", "Run"], versions)
    if length(versions) > 1
        append!(
            header_parts,
            ["Δ " * versions[idx] * " vs prev" for idx in 2:length(versions)],
        )
    end
    println(buffer, "| ", join(header_parts, " | "), " |")
    println(buffer, "| ", join(fill("---", length(header_parts)), " | "), " |")

    entries_sorted = sort(collect(entries); by = entry -> (entry[1], entry[3], entry[4]))
    for (search, dataset, gene, run, metric_name) in entries_sorted
        values = Vector{Any}(undef, length(versions))
        for (idx, version) in enumerate(versions)
            searches_entry = get(version_data, version, Dict{String, Any}())
            metrics_by_search = searches_entry isa AbstractDict ?
                get(searches_entry, search, Dict{String, Any}()) :
                Dict{String, Any}()
            metrics = metrics_by_search isa AbstractDict ?
                get(metrics_by_search, dataset, Dict{String, Any}()) :
                Dict{String, Any}()
            values[idx] = metrics isa AbstractDict ? get(metrics, metric_name, missing) : missing
        end

        has_value = any(value -> !(value === missing || value === nothing), values)
        has_value || continue

        deltas = String[]
        if length(values) > 1
            prev_value = values[1]
            for idx in 2:length(values)
                push!(deltas, format_delta(values[idx], prev_value))
                prev_value = values[idx]
            end
        end

        row_parts = vcat(
            [search, gene, run],
            [format_value(value) for value in values],
            deltas,
        )
        println(buffer, "| ", join(row_parts, " | "), " |")
    end
    println(buffer, "")
end

function write_keap1_tables(
    buffer::IOBuffer,
    keap1_entries::Dict{String, Set{Tuple{String, String, String, String, String}}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    for group in ("precursors", "protein_groups")
        entries = get(keap1_entries, group, Set{Tuple{String, String, String, String, String}}())
        write_keap1_table(buffer, group, entries, version_data, versions)
    end
end

function build_report(version_data::AbstractDict{String, Any}, versions::Vector{String})
    buffer = IOBuffer()
    println(buffer, "### Regression Metrics Report")
    println(buffer, "")
    println(buffer, "- Versions: ", join(versions, ", "))
    println(buffer, "")

    dataset_entries = Set{Tuple{String, String}}()
    for version in versions
        searches_entry = get(version_data, version, Dict{String, Any}())
        if searches_entry isa AbstractDict
            for (search, datasets) in pairs(searches_entry)
                datasets isa AbstractDict || continue
                for dataset in keys(datasets)
                    push!(dataset_entries, (String(search), String(dataset)))
                end
            end
        end
    end
    entries_sorted = sort(collect(dataset_entries); by = entry -> (entry[1], entry[2]))

    all_metrics = Set{String}()
    for version in versions
        searches_entry = get(version_data, version, Dict{String, Any}())
        searches_entry isa AbstractDict || continue
        for (search, datasets) in pairs(searches_entry)
            datasets isa AbstractDict || continue
            for dataset in keys(datasets)
                metrics = get(datasets, dataset, Dict{String, Any}())
                metrics isa AbstractDict && union!(all_metrics, keys(metrics))
            end
        end
    end

    filtered_metrics = filter(
        metric -> include_metric(metric) &&
            parse_fold_change_metric(metric) === nothing &&
            parse_fold_change_fc_variance_metric(metric) === nothing &&
            !startswith(metric, "keap1."),
        collect(all_metrics),
    )
    fold_change_entries = collect_fold_change_entries(version_data, versions)
    fold_change_fc_variance_entries = collect_fold_change_fc_variance_entries(version_data, versions)
    keap1_entries = collect_keap1_entries(version_data, versions)
    order_map = metric_group_order()
    fold_change_rank = get(order_map, "fold_change", length(order_map) + 1)
    fold_change_fc_variance_rank = fold_change_rank + 1
    keap1_rank = get(order_map, "keap1", length(order_map) + 1)
    special_blocks = sort(
        [(keap1_rank, :keap1), (fold_change_rank, :fold_change), (fold_change_fc_variance_rank, :fold_change_fc_variance)];
        by = first,
    )
    special_written = Dict(:keap1 => false, :fold_change => false, :fold_change_fc_variance => false)

    for metric in ordered_metrics(filtered_metrics)
        metric_rank = get(
            order_map,
            normalized_group(metric_group(metric)),
            length(order_map) + 1,
        )
        for (rank, label) in special_blocks
            if !special_written[label] && metric_rank > rank
                if label == :keap1
                    write_keap1_tables(buffer, keap1_entries, version_data, versions)
                elseif label == :fold_change
                    write_fold_change_tables(buffer, fold_change_entries, version_data, versions)
                elseif label == :fold_change_fc_variance
                    write_fold_change_fc_variance_tables(buffer, fold_change_fc_variance_entries, version_data, versions)
                end
                special_written[label] = true
            end
        end
        println(buffer, "**Metric:** ", metric)
        println(buffer, "")
        header_parts = vcat(["Search"], versions)
        if length(versions) > 1
            append!(
                header_parts,
                ["Δ " * versions[idx] * " vs prev" for idx in 2:length(versions)],
            )
            group = normalized_group(metric_group(metric))
            if group in ("identification", "runtime")
                append!(
                    header_parts,
                    ["% " * versions[idx] * " vs prev" for idx in 2:length(versions)],
                )
            end
        end
        println(buffer, "| ", join(header_parts, " | "), " |")
        println(buffer, "| ", join(fill("---", length(header_parts)), " | "), " |")

        for (search, dataset) in entries_sorted
            values = Vector{Any}(undef, length(versions))
            for (idx, version) in enumerate(versions)
                searches_entry = get(version_data, version, Dict{String, Any}())
                metrics_by_search = searches_entry isa AbstractDict ?
                    get(searches_entry, search, Dict{String, Any}()) :
                    Dict{String, Any}()
                metrics = metrics_by_search isa AbstractDict ?
                    get(metrics_by_search, dataset, Dict{String, Any}()) :
                    Dict{String, Any}()
                values[idx] = metrics isa AbstractDict ? get(metrics, metric, missing) : missing
            end

            has_value = any(value -> !(value === missing || value === nothing), values)
            has_value || continue

            deltas = String[]
            if length(values) > 1
                prev_value = values[1]
                for idx in 2:length(values)
                    push!(deltas, format_delta(values[idx], prev_value))
                    prev_value = values[idx]
                end
            end

            percent_deltas = String[]
            group = normalized_group(metric_group(metric))
            if length(values) > 1 && group in ("identification", "runtime")
                prev_value = values[1]
                for idx in 2:length(values)
                    push!(percent_deltas, format_percent_delta(values[idx], prev_value))
                    prev_value = values[idx]
                end
            end

            row_parts = vcat(
                [search],
                [format_value(value) for value in values],
                deltas,
                percent_deltas,
            )
            println(buffer, "| ", join(row_parts, " | "), " |")
        end
        println(buffer, "")
    end
    for (rank, label) in special_blocks
        if !special_written[label]
            if label == :keap1
                write_keap1_tables(buffer, keap1_entries, version_data, versions)
            elseif label == :fold_change
                write_fold_change_tables(buffer, fold_change_entries, version_data, versions)
            elseif label == :fold_change_fc_variance
                write_fold_change_fc_variance_tables(buffer, fold_change_fc_variance_entries, version_data, versions)
            end
            special_written[label] = true
        end
    end
    String(take!(buffer))
end

function main()
    release_root = get(ENV, "PIONEER_METRICS_RELEASE_ROOT", "")
    develop_root = get(ENV, "PIONEER_METRICS_DEVELOP_ROOT", "")
    current_root = get(ENV, "PIONEER_METRICS_CURRENT_ROOT", "")
    output_path = get(ENV, "PIONEER_REPORT_OUTPUT", "")

    isempty(release_root) && error("PIONEER_METRICS_RELEASE_ROOT not set")
    isempty(develop_root) && error("PIONEER_METRICS_DEVELOP_ROOT not set")
    isempty(current_root) && error("PIONEER_METRICS_CURRENT_ROOT not set")
    isempty(output_path) && error("PIONEER_REPORT_OUTPUT not set")

    versions = sorted_release_versions(release_root)
    push!(versions, "develop")
    push!(versions, "current")

    version_data = Dict{String, Any}()
    for version in versions
        if version == "develop"
            version_data[version] = collect_metrics(develop_root)
        elseif version == "current"
            version_data[version] = collect_metrics(current_root)
        else
            version_root = joinpath(release_root, version)
            version_data[version] = collect_metrics(version_root)
        end
    end

    report = build_report(version_data, versions)

    output_dir = dirname(output_path)
    isdir(output_dir) || mkpath(output_dir)
    open(output_path, "w") do io
        write(io, report)
    end
end

main()
