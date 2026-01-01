#!/usr/bin/env julia

using JSON
using Printf

const MAX_FDR_PLOTS = 4

const NA = ""
const TEMPLATE_DIR = joinpath(@__DIR__, "templates")

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
        return (; files, scan_root = root)
    end
    onerror = err -> begin
        @warn "Skipping missing metrics directory" error = err
        return
    end

    for (dir, _, filenames) in walkdir(root; onerror = onerror)
        for filename in filenames
            if startswith(filename, "metrics_") && endswith(filename, ".json")
                push!(files, joinpath(dir, filename))
            end
        end
    end

    (; files, scan_root = root)
end

function parse_dataset_search(path::AbstractString, root::AbstractString)
    rel_path = relpath(path, root)
    parts = splitpath(rel_path)
    base = replace(basename(path), r"^metrics_" => "", r"\.json$" => "")
    dataset_dir = length(parts) >= 2 ? parts[end - 1] : ""
    search = ""

    dataset_from_name = ""
    search_from_name = ""
    matched = match(r"^(.+)_search_(.+)$", base)
    if matched !== nothing
        dataset_from_name = matched.captures[1]
        search_from_name = matched.captures[2]
    end

    if !isempty(dataset_dir)
        prefix = dataset_dir * "_"
        if startswith(base, prefix)
            search = base[length(prefix) + 1:end]
        end
        dataset = dataset_dir
    else
        dataset = isempty(dataset_from_name) ? base : dataset_from_name
    end

    if isempty(search) && !isempty(search_from_name)
        search = "search_" * search_from_name
    end

    dataset, search
end

function collect_metrics(root::AbstractString)
    results = Dict{String, Dict{String, Dict{String, Float64}}}()
    summary = metrics_files(root)
    metrics_count = length(summary.files)
    duplicate_count = 0
    searches_seen = String[]
    for path in summary.files
        dataset, search = parse_dataset_search(path, root)
        push!(searches_seen, search)

        metrics_json = nothing
        try
            metrics_json = JSON.parsefile(path; dicttype = Dict)
        catch err
            @warn "Failed to parse metrics file; leaving metrics as NA" path=path error=err
            continue
        end
        flat = flatten_metrics(metrics_json)
        search_entry = get!(results, search, Dict{String, Dict{String, Float64}}())
        if haskey(search_entry, dataset)
            duplicate_count += 1
            @warn "Duplicate dataset/search parsed from metrics file; overwriting prior entry" dataset = dataset search = search path = path
        end
        search_entry[dataset] = flat
    end
    if metrics_count > 0
        unique_searches = unique(searches_seen)
        if all(s -> s in ("results", "unknown-search"), unique_searches)
            @warn "All parsed searches collapsed to results/unknown-search; metrics root may be too high" root = root searches = unique_searches
        end
        results_count = count(==("results"), searches_seen)
        if results_count > 0 && results_count / metrics_count >= 0.8
            @warn "Most searches parsed as results; metrics root may be one level too high" root = root results_search_count = results_count total = metrics_count
        end
    end

    return (;
        results,
        metrics_count,
        scan_root = summary.scan_root,
        duplicate_count,
    )
end

function run_parse_check()
    sample_root = "/tmp/pioneer-metrics-root"
    samples = [
        joinpath(sample_root, "results", "Example_Dataset", "metrics_Example_Dataset_search_alpha.json"),
        joinpath(sample_root, "Example_Dataset", "metrics_Example_Dataset_search_alpha.json"),
        joinpath(sample_root, "results", "MTAC_Yeast_Alternating_5min", "metrics_MTAC_Yeast_Alternating_5min_search_entrap.json"),
    ]
    println("Parsing sample metrics paths:")
    for path in samples
        dataset, search = parse_dataset_search(path, sample_root)
        println("  ", path, " -> dataset=", dataset, ", search=", search)
    end
end

function slugify_label(label::AbstractString)
    replace(label, r"[^A-Za-z0-9_-]" => "_")
end

function html_escape(text::AbstractString)
    escaped = replace(text, "&" => "&amp;")
    escaped = replace(escaped, "<" => "&lt;")
    escaped = replace(escaped, ">" => "&gt;")
    escaped = replace(escaped, "\"" => "&quot;")
    replace(escaped, "'" => "&#39;")
end

function next_table_id(counter::Base.RefValue{Int}, prefix::AbstractString)
    counter[] += 1
    string(prefix, "-", counter[])
end

function load_template(name::AbstractString)
    path = joinpath(TEMPLATE_DIR, name)
    isfile(path) || error("Template not found: " * path)
    read(path, String)
end

function render_template(template::AbstractString, replacements::Dict{String, String})
    rendered = template
    for (key, value) in replacements
        rendered = replace(rendered, "{{" * key * "}}" => value)
    end
    rendered
end

function print_table_headers(
    buffer::IOBuffer,
    headers::Vector{String},
    numeric_start::Int,
)
    println(buffer, "<thead><tr>")
    for (idx, header) in enumerate(headers)
        class_attr = idx >= numeric_start ? " class=\"numeric\"" : ""
        println(buffer, "<th", class_attr, ">", html_escape(header), "</th>")
    end
    println(buffer, "</tr></thead>")
end

function print_table_row(
    buffer::IOBuffer,
    row_parts::Vector{String},
    numeric_start::Int;
    dataset::AbstractString = "",
    search::AbstractString = "",
)
    dataset_attr = isempty(dataset) ? "" : " data-dataset=\"" * html_escape(dataset) * "\""
    search_attr = isempty(search) ? "" : " data-search=\"" * html_escape(search) * "\""
    println(buffer, "<tr", dataset_attr, search_attr, ">")
    for (idx, value) in enumerate(row_parts)
        class_attr = idx >= numeric_start ? " class=\"numeric\"" : ""
        println(buffer, "<td", class_attr, ">", html_escape(value), "</td>")
    end
    println(buffer, "</tr>")
end

function print_table_controls(
    buffer::IOBuffer,
    table_id::AbstractString;
    chart_mode::AbstractString = "",
)
    if !isempty(chart_mode)
        println(
            buffer,
            "<div class=\"table-chart\" data-table-id=\"",
            html_escape(table_id),
            "\" data-chart-mode=\"",
            html_escape(chart_mode),
            "\"></div>",
        )
    end
    println(
        buffer,
        "<div class=\"table-controls\"><label>Filter: ",
        "<input class=\"table-filter\" type=\"search\" placeholder=\"Filter rows\" data-table-id=\"",
        html_escape(table_id),
        "\"></label></div>",
    )
end

function fdr_plot_paths(metrics_dir::AbstractString)
    plots = String[]
    if isdir(metrics_dir)
        for entry in readdir(metrics_dir; join = true)
            if isfile(entry) && endswith(lowercase(entry), ".png")
                push!(plots, entry)
            end
        end
    end

    entrapment_dir = joinpath(metrics_dir, "entrapment_analysis")
    if isdir(entrapment_dir)
        for entry in readdir(entrapment_dir; join = true)
            if isfile(entry) && endswith(lowercase(entry), ".png")
                push!(plots, entry)
            end
        end
    end

    unique!(plots)
    sort!(plots)
    plots
end

function collect_fdr_plots(root::AbstractString; layout::Symbol)
    plots_by_search = Dict{String, Dict{String, Vector{String}}}()
    summary = metrics_files(root)
    for path in summary.files
        dataset, search = parse_dataset_search(path, root; layout = layout)
        isempty(search) && continue
        plots = fdr_plot_paths(dirname(path))
        isempty(plots) && continue
        search_entry = get!(plots_by_search, search, Dict{String, Vector{String}}())
        search_entry[dataset] = plots
    end
    plots_by_search
end

function copy_plot(path::AbstractString, dest_dir::AbstractString)
    mkpath(dest_dir)
    dest_path = joinpath(dest_dir, basename(path))
    cp(path, dest_path; force = true)
    dest_path
end

function build_plots_section(
    plots_by_search::Dict{String, Dict{String, Vector{String}}},
    output_dir::AbstractString,
    plot_root::AbstractString,
)
    buffer = IOBuffer()
    for search in sort(collect(keys(plots_by_search)))
        println(buffer, "<div class=\"search-block\">")
        datasets = get(plots_by_search, search, Dict{String, Vector{String}}())
        for dataset in sort(collect(keys(datasets)))
            plots = datasets[dataset]
            isempty(plots) && continue
            println(buffer, "<div class=\"dataset-block\">")
            println(buffer, "<h3>", html_escape(dataset), "</h3>")
            println(buffer, "<div class=\"plot-grid\">")
            for plot_path in plots[1:min(length(plots), MAX_FDR_PLOTS)]
                dest_dir = joinpath(plot_root, slugify_label(search), slugify_label(dataset))
                copied = copy_plot(plot_path, dest_dir)
                rel_path = relpath(copied, output_dir)
                println(buffer, "<div class=\"plot-card\">")
                println(
                    buffer,
                    "<img src=\"",
                    html_escape(rel_path),
                    "\" alt=\"",
                    html_escape(basename(plot_path)),
                    "\">",
                )
                println(buffer, "<div class=\"plot-caption\">", html_escape(basename(plot_path)), "</div>")
                println(buffer, "</div>")
            end
            println(buffer, "</div>")
            println(buffer, "</div>")
        end
        println(buffer, "</div>")
    end
    String(take!(buffer))
end

function build_html_report(
    plots_by_search::Dict{String, Dict{String, Vector{String}}},
    output_path::AbstractString,
    metrics_report::AbstractString,
)
    output_dir = dirname(output_path)
    plot_root = joinpath(output_dir, "fdr_plots")
    mkpath(output_dir)
    mkpath(plot_root)

    template = load_template("report.html")
    style = load_template("report.css")
    script = load_template("report.js")
    plots_section = build_plots_section(plots_by_search, output_dir, plot_root)
    render_template(
        template,
        Dict(
            "TITLE" => "Regression Metrics eFDR Plots",
            "STYLE" => style,
            "SCRIPT" => script,
            "METRICS_REPORT" => metrics_report,
            "PLOTS_SECTION" => plots_section,
        ),
    )
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

function format_abs_delta(current, previous)
    if current === missing || previous === missing || current === nothing || previous === nothing
        return NA
    elseif current isa Number && previous isa Number
        delta = abs(Float64(current)) - abs(Float64(previous))
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
    table_counter::Base.RefValue{Int},
)
    isempty(entries) && return
    println(buffer, "<div class=\"metric-block\">")
    println(buffer, "<h3>", html_escape("fold_change.error." * group), "</h3>")

    header_parts = vcat(["Search", "Species", "Condition"], versions)
    if length(versions) > 1
        append!(
            header_parts,
            ["Δ " * versions[idx] * " vs " * versions[idx - 1] for idx in 2:length(versions)],
        )
    end
    initial_sort = length(versions) > 1 ? string(length(header_parts) - 1) : ""
    table_id = next_table_id(table_counter, "fold-change-" * slugify_label(group))
    print_table_controls(buffer, table_id; chart_mode = "delta")
    println(
        buffer,
        "<table class=\"metrics-table\" data-initial-sort=\"",
        initial_sort,
        "\" data-versions=\"",
        html_escape(join(versions, "|")),
        "\" data-metric-group=\"fold_change",
        "\" id=\"",
        html_escape(table_id),
        "\">",
    )
    print_table_headers(buffer, header_parts, 4)
    println(buffer, "<tbody>")

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
                push!(deltas, format_abs_delta(values[idx], prev_value))
                prev_value = values[idx]
            end
        end

        row_parts = vcat(
            [search, format_species_label(species), format_condition_label(condition)],
            [format_value(value) for value in values],
            deltas,
        )
        print_table_row(
            buffer,
            row_parts,
            4;
            dataset = dataset,
            search = search,
        )
    end
    println(buffer, "</tbody></table>")
    println(buffer, "</div>")
end

function write_fold_change_tables(
    buffer::IOBuffer,
    fold_change_entries::Dict{String, Set{Tuple{String, String, String, String}}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
    table_counter::Base.RefValue{Int},
)
    for group in ("precursors", "protein_groups")
        entries = get(fold_change_entries, group, Set{Tuple{String, String, String, String}}())
        write_fold_change_table(buffer, group, entries, version_data, versions, table_counter)
    end
end

function write_fold_change_fc_variance_table(
    buffer::IOBuffer,
    group::AbstractString,
    entries::Set{Tuple{String, String, String, String}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
    table_counter::Base.RefValue{Int},
)
    isempty(entries) && return
    println(buffer, "<div class=\"metric-block\">")
    println(buffer, "<h3>", html_escape("fold_change.variance." * group), "</h3>")

    header_parts = vcat(["Search", "Species", "Condition"], versions)
    if length(versions) > 1
        append!(
            header_parts,
            ["Δ " * versions[idx] * " vs " * versions[idx - 1] for idx in 2:length(versions)],
        )
    end
    initial_sort = length(versions) > 1 ? string(length(header_parts) - 1) : ""
    table_id = next_table_id(table_counter, "fold-change-variance-" * slugify_label(group))
    print_table_controls(buffer, table_id; chart_mode = "delta")
    println(
        buffer,
        "<table class=\"metrics-table\" data-initial-sort=\"",
        initial_sort,
        "\" data-versions=\"",
        html_escape(join(versions, "|")),
        "\" data-metric-group=\"fold_change_variance",
        "\" id=\"",
        html_escape(table_id),
        "\">",
    )
    print_table_headers(buffer, header_parts, 4)
    println(buffer, "<tbody>")

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
        print_table_row(
            buffer,
            row_parts,
            4;
            dataset = dataset,
            search = search,
        )
    end
    println(buffer, "</tbody></table>")
    println(buffer, "</div>")
end

function write_fold_change_fc_variance_tables(
    buffer::IOBuffer,
    fold_change_fc_variance_entries::Dict{String, Set{Tuple{String, String, String, String}}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
    table_counter::Base.RefValue{Int},
)
    for group in ("precursors", "protein_groups")
        entries = get(fold_change_fc_variance_entries, group, Set{Tuple{String, String, String, String}}())
        write_fold_change_fc_variance_table(buffer, group, entries, version_data, versions, table_counter)
    end
end

function write_keap1_table(
    buffer::IOBuffer,
    group::AbstractString,
    entries::Set{Tuple{String, String, String, String, String}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
    table_counter::Base.RefValue{Int},
)
    isempty(entries) && return
    println(buffer, "<div class=\"metric-block\">")
    println(buffer, "<h3>", html_escape("keap1." * group), "</h3>")

    header_parts = vcat(["Search", "Gene", "Run"], versions)
    if length(versions) > 1
        append!(
            header_parts,
            ["Δ " * versions[idx] * " vs " * versions[idx - 1] for idx in 2:length(versions)],
        )
    end
    initial_sort = length(versions) > 1 ? string(length(header_parts) - 1) : ""
    table_id = next_table_id(table_counter, "keap1-" * slugify_label(group))
    print_table_controls(buffer, table_id)
    println(
        buffer,
        "<table class=\"metrics-table\" data-initial-sort=\"",
        initial_sort,
        "\" data-versions=\"",
        html_escape(join(versions, "|")),
        "\" data-metric-group=\"keap1",
        "\" id=\"",
        html_escape(table_id),
        "\">",
    )
    print_table_headers(buffer, header_parts, 4)
    println(buffer, "<tbody>")

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
        print_table_row(
            buffer,
            row_parts,
            4;
            dataset = dataset,
            search = search,
        )
    end
    println(buffer, "</tbody></table>")
    println(buffer, "</div>")
end

function write_keap1_tables(
    buffer::IOBuffer,
    keap1_entries::Dict{String, Set{Tuple{String, String, String, String, String}}},
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
    table_counter::Base.RefValue{Int},
)
    for group in ("precursors", "protein_groups")
        entries = get(keap1_entries, group, Set{Tuple{String, String, String, String, String}}())
        write_keap1_table(buffer, group, entries, version_data, versions, table_counter)
    end
end

function build_report(
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
)
    buffer = IOBuffer()
    table_counter = Ref(0)
    println(buffer, "<div class=\"metric-block\">")
    println(buffer, "<h2>Regression Metrics Report</h2>")
    println(
        buffer,
        "<p><strong>Versions:</strong> ",
        html_escape(join(versions, ", ")),
        "</p>",
    )
    println(buffer, "</div>")

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
                    write_keap1_tables(buffer, keap1_entries, version_data, versions, table_counter)
                elseif label == :fold_change
                    write_fold_change_tables(buffer, fold_change_entries, version_data, versions, table_counter)
                elseif label == :fold_change_fc_variance
                    write_fold_change_fc_variance_tables(
                        buffer,
                        fold_change_fc_variance_entries,
                        version_data,
                        versions,
                        table_counter,
                    )
                end
                special_written[label] = true
            end
        end
        println(buffer, "<div class=\"metric-block\">")
        println(buffer, "<h3>Metric: ", html_escape(metric), "</h3>")
        header_parts = vcat(["Search"], versions)
        if length(versions) > 1
            current_version = versions[end]
            previous_versions = versions[1:(end - 1)]
            append!(
                header_parts,
                ["Δ " * current_version * " vs " * version for version in previous_versions],
            )
            group = normalized_group(metric_group(metric))
            if group in ("identification", "runtime")
                append!(
                    header_parts,
                    ["% " * current_version * " vs " * version for version in previous_versions],
                )
            end
        end
        initial_sort = ""
        if length(versions) > 1
            base_columns = 1
            delta_index = base_columns + length(versions) + 1
            initial_sort = string(delta_index)
        end
        chart_mode = ""
        group = normalized_group(metric_group(metric))
        if group == "identification" || group == "runtime"
            chart_mode = "percent_delta"
        elseif group == "entrapment"
            chart_mode = "raw"
        elseif group == "cv"
            chart_mode = "delta"
        end
        table_id = next_table_id(table_counter, slugify_label(metric))
        print_table_controls(buffer, table_id; chart_mode = chart_mode)
        println(
            buffer,
            "<table class=\"metrics-table\" data-initial-sort=\"",
            initial_sort,
            "\" data-versions=\"",
            html_escape(join(versions, "|")),
            "\" data-metric-group=\"",
            html_escape(group),
            "\" id=\"",
            html_escape(table_id),
            "\">",
        )
        print_table_headers(buffer, header_parts, 2)
        println(buffer, "<tbody>")

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
                current_value = values[end]
                for idx in 1:(length(values) - 1)
                    push!(deltas, format_delta(current_value, values[idx]))
                end
            end

            percent_deltas = String[]
            if length(values) > 1 && group in ("identification", "runtime")
                current_value = values[end]
                for idx in 1:(length(values) - 1)
                    push!(percent_deltas, format_percent_delta(current_value, values[idx]))
                end
            end

            row_parts = vcat(
                [search],
                [format_value(value) for value in values],
                deltas,
                percent_deltas,
            )
            print_table_row(
                buffer,
                row_parts,
                2;
                dataset = dataset,
                search = search,
            )
        end
        println(buffer, "</tbody></table>")
        println(buffer, "</div>")
    end
    for (rank, label) in special_blocks
        if !special_written[label]
            if label == :keap1
                write_keap1_tables(buffer, keap1_entries, version_data, versions, table_counter)
            elseif label == :fold_change
                write_fold_change_tables(buffer, fold_change_entries, version_data, versions, table_counter)
            elseif label == :fold_change_fc_variance
                write_fold_change_fc_variance_tables(
                    buffer,
                    fold_change_fc_variance_entries,
                    version_data,
                    versions,
                    table_counter,
                )
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
    metrics_root = get(ENV, "PIONEER_METRICS_ROOT", "")
    output_path = get(ENV, "PIONEER_REPORT_OUTPUT", "")
    html_output_path = get(ENV, "PIONEER_HTML_REPORT_OUTPUT", "")
    parse_check = get(ENV, "PIONEER_REPORT_PARSE_CHECK", "false") == "true"

    if parse_check
        run_parse_check()
        return
    end

    if !isempty(metrics_root)
        isempty(release_root) && (release_root = joinpath(metrics_root, "release"))
        isempty(develop_root) && (develop_root = joinpath(metrics_root, "develop"))
        isempty(current_root) && (current_root = joinpath(metrics_root, "results"))
    end

    plots_root = get(ENV, "PIONEER_FDR_PLOTS_ROOT", current_root)

    isempty(release_root) && error("PIONEER_METRICS_RELEASE_ROOT not set")
    isempty(develop_root) && error("PIONEER_METRICS_DEVELOP_ROOT not set")
    isempty(current_root) && error("PIONEER_METRICS_CURRENT_ROOT not set")
    if isempty(html_output_path) && isempty(output_path)
        error("PIONEER_HTML_REPORT_OUTPUT not set and PIONEER_REPORT_OUTPUT not set")
    end

    versions = sorted_release_versions(release_root)
    push!(versions, "develop")
    push!(versions, "current")

    version_data = Dict{String, Any}()
    for version in versions
        if version == "develop"
            summary = collect_metrics(develop_root)
            version_data[version] = summary.results
            @info "Collected metrics" version = version root = develop_root count = summary.metrics_count scan_root = summary.scan_root
        elseif version == "current"
            summary = collect_metrics(current_root)
            version_data[version] = summary.results
            @info "Collected metrics" version = version root = current_root count = summary.metrics_count scan_root = summary.scan_root
        else
            version_root = joinpath(release_root, version)
            summary = collect_metrics(version_root)
            version_data[version] = summary.results
            @info "Collected metrics" version = version root = version_root count = summary.metrics_count scan_root = summary.scan_root
        end
    end

    report = build_report(version_data, versions)
    if isempty(html_output_path)
        base, _ = splitext(output_path)
        html_output_path = base * ".html"
    end

    plots_by_search = Dict{String, Dict{String, Vector{String}}}()
    if isdir(plots_root)
        plots_by_search = collect_fdr_plots(plots_root; layout = :results)
    else
        @warn "Plots root not found; writing HTML report without plots" plots_root=plots_root
    end

    html_report = build_html_report(plots_by_search, html_output_path, report)
    open(html_output_path, "w") do io
        write(io, html_report)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
