#!/usr/bin/env julia

using JSON
using Printf

const NA = "NA"

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

function build_report(version_data::AbstractDict{String, Any}, versions::Vector{String})
    buffer = IOBuffer()
    println(buffer, "# Regression Metrics Report")
    println(buffer, "")
    println(buffer, "- Versions: ", join(versions, ", "))
    println(buffer, "")

    searches = Set{String}()
    for version in versions
        searches_entry = get(version_data, version, Dict{String, Any}())
        if searches_entry isa AbstractDict
            union!(searches, keys(searches_entry))
        end
    end
    searches_sorted = sort(collect(searches))
    for search in searches_sorted
        println(buffer, "## Search: ", search)
        println(buffer, "")

        datasets = Set{String}()
        for version in versions
            searches_entry = get(version_data, version, Dict{String, Any}())
            metrics_by_search = searches_entry isa AbstractDict ?
                get(searches_entry, search, Dict{String, Any}()) :
                Dict{String, Any}()
            metrics_by_search isa AbstractDict && union!(datasets, keys(metrics_by_search))
        end
        for dataset in sort(collect(datasets))
            println(buffer, "### Dataset: ", dataset)
            println(buffer, "")
            println(buffer, "| Metric | Version | Value | Δ vs prev |")
            println(buffer, "| --- | --- | --- | --- |")

            all_metrics = Set{String}()
            for version in versions
                searches_entry = get(version_data, version, Dict{String, Any}())
                metrics_by_search = searches_entry isa AbstractDict ?
                    get(searches_entry, search, Dict{String, Any}()) :
                    Dict{String, Any}()
                metrics = metrics_by_search isa AbstractDict ?
                    get(metrics_by_search, dataset, Dict{String, Any}()) :
                    Dict{String, Any}()
                metrics isa AbstractDict && union!(all_metrics, keys(metrics))
            end

            for metric in sort(collect(all_metrics))
                prev_value = missing
                for (idx, version) in enumerate(versions)
                    searches_entry = get(version_data, version, Dict{String, Any}())
                    metrics_by_search = searches_entry isa AbstractDict ?
                        get(searches_entry, search, Dict{String, Any}()) :
                        Dict{String, Any}()
                    metrics = metrics_by_search isa AbstractDict ?
                        get(metrics_by_search, dataset, Dict{String, Any}()) :
                        Dict{String, Any}()
                    value = metrics isa AbstractDict ? get(metrics, metric, missing) : missing
                    delta = idx == 1 ? NA : format_delta(value, prev_value)
                    println(
                        buffer,
                        "| ",
                        metric,
                        " | ",
                        version,
                        " | ",
                        format_value(value),
                        " | ",
                        delta,
                        " |",
                    )
                    prev_value = value
                end
            end
            println(buffer, "")
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
