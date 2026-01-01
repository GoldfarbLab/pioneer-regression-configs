#!/usr/bin/env julia

using JSON
using Printf

const MAX_FDR_PLOTS = 4

const NA = ""

# Metrics report supports both layouts:
# - <root>/results/<dataset>/metrics_<dataset>_<search>.json
# - <root>/<dataset>/metrics_<dataset>_<search>.json
# Search names are derived from the filename suffix (last underscore) in both cases.

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
        return (; files, scan_root = root, layout = "unknown", mixed_layout = false)
    end
    onerror = err -> begin
        @warn "Skipping missing metrics directory" error = err
        return
    end

    function collect_files(search_root::AbstractString; skip_results::Bool = false)
        collected = String[]
        for (dir, dirs, filenames) in walkdir(search_root; onerror = onerror)
            if skip_results && dir == search_root
                filter!(name -> name != "results", dirs)
            end
            for filename in filenames
                if startswith(filename, "metrics_") && endswith(filename, ".json")
                    push!(collected, joinpath(dir, filename))
                end
            end
        end
        collected
    end

    results_dir = joinpath(root, "results")
    files_in_root = collect_files(root; skip_results = true)
    files_in_results = isdir(results_dir) ? collect_files(results_dir) : String[]

    if !isempty(files_in_results) && !isempty(files_in_root)
        @warn "Found metrics in both results and flat layouts; prioritizing results subtree" root = root results_count = length(files_in_results) flat_count = length(files_in_root)
        return (; files = files_in_results, scan_root = results_dir, layout = "results/<dataset>", mixed_layout = true)
    elseif !isempty(files_in_results)
        return (; files = files_in_results, scan_root = results_dir, layout = "results/<dataset>", mixed_layout = false)
    elseif !isempty(files_in_root)
        return (; files = files_in_root, scan_root = root, layout = "flat <dataset>", mixed_layout = false)
    end

    if isdir(results_dir)
        return (; files, scan_root = results_dir, layout = "results/<dataset>", mixed_layout = false)
    end
    (; files, scan_root = root, layout = "flat <dataset>", mixed_layout = false)
end

function truthy_env(value::AbstractString)
    lowered = lowercase(strip(value))
    lowered in ("1", "true", "yes", "on")
end

function parse_metrics_filename(path::AbstractString)
    base = replace(basename(path), r"^metrics_" => "", r"\.json$" => "")
    if isempty(base)
        return (; dataset = "", search = "", has_search = false)
    end
    idx = findlast('_', base)
    if idx === nothing || idx == 1 || idx == lastindex(base)
        return (; dataset = base, search = "", has_search = false)
    end
    dataset = base[1:(idx - 1)]
    search = base[(idx + 1):end]
    (; dataset, search, has_search = !isempty(search))
end

# Metrics filenames follow metrics_<dataset>_<search>.json; search is derived from the
# filename suffix for both supported layouts.
function infer_dataset_search(rel_path::AbstractString; skip_first_segment::Bool = false)
    parts = splitpath(rel_path)
    filename_info = parse_metrics_filename(rel_path)
    if length(parts) == 1
        dataset = isempty(filename_info.dataset) ? basename(rel_path) : filename_info.dataset
        @warn "Metrics path has no directory segments; using filename dataset and unknown search" rel_path = rel_path dataset = dataset
        return (;
            dataset,
            search = "unknown-search",
            layout = "filename-only",
            skipped_results = false,
            filename_only = true,
            forced_skip = skip_first_segment,
            filename_dataset = filename_info.dataset,
            filename_search = filename_info.search,
        )
    end

    dataset_index = 1
    skipped_results = false
    if skip_first_segment && length(parts) >= 2
        dataset_index = 2
    elseif parts[1] == "results" && length(parts) >= 2
        dataset_index = 2
        skipped_results = true
    end

    dataset_dir = parts[dataset_index]
    if endswith(dataset_dir, ".json")
        dataset = isempty(filename_info.dataset) ? dataset_dir : filename_info.dataset
        @warn "Metrics path missing dataset directory; using filename dataset and unknown search" rel_path = rel_path dataset = dataset
        return (;
            dataset,
            search = "unknown-search",
            layout = "filename-only",
            skipped_results,
            filename_only = true,
            forced_skip = skip_first_segment,
            filename_dataset = filename_info.dataset,
            filename_search = filename_info.search,
        )
    end
    layout = dataset_index == 2 ? "results/<dataset>" : "flat <dataset>"
    dataset = dataset_dir
    search = filename_info.has_search ? filename_info.search : "unknown-search"

    if !isempty(filename_info.dataset) && filename_info.dataset != dataset_dir
        @warn "Dataset name in filename does not match dataset directory; using directory name" rel_path = rel_path filename_dataset = filename_info.dataset dataset_dir = dataset_dir
    end
    if !filename_info.has_search
        @warn "Metrics filename missing search suffix; using unknown-search" rel_path = rel_path dataset = dataset
    end

    (;
        dataset,
        search,
        layout,
        skipped_results,
        filename_only = false,
        forced_skip = skip_first_segment,
        filename_dataset = filename_info.dataset,
        filename_search = filename_info.search,
    )
end

# Parse metrics paths using metrics_<dataset>_<search>.json naming and dual layouts.
function parse_dataset_search(path::AbstractString, root::AbstractString; return_info::Bool = false)
    rel_path = relpath(path, root)
    skip_first_segment = truthy_env(get(ENV, "PIONEER_REPORT_SKIP_FIRST_SEGMENT", "false"))
    info = infer_dataset_search(rel_path; skip_first_segment = skip_first_segment)
    if return_info
        return info.dataset, info.search, info
    end
    info.dataset, info.search
end

function collect_metrics(root::AbstractString)
    results = Dict{String, Dict{String, Dict{String, Float64}}}()
    summary = metrics_files(root)
    metrics_count = length(summary.files)
    layout_counts = Dict{String, Int}()
    skipped_results_count = 0
    filename_only_count = 0
    forced_skip_count = 0
    duplicate_count = 0
    searches_seen = String[]
    for path in summary.files
        dataset, search, info = parse_dataset_search(path, root; return_info = true)
        layout_counts[info.layout] = get(layout_counts, info.layout, 0) + 1
        skipped_results_count += info.skipped_results ? 1 : 0
        filename_only_count += info.filename_only ? 1 : 0
        forced_skip_count += info.forced_skip ? 1 : 0
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

    layout_summary = if !isempty(layout_counts)
        maximum(collect(keys(layout_counts)); by = key -> layout_counts[key])
    else
        summary.layout
    end
    warning_needed = skipped_results_count > 0 || filename_only_count > 0 || forced_skip_count > 0
    return (;
        results,
        metrics_count,
        layout = layout_summary,
        scan_root = summary.scan_root,
        mixed_layout = summary.mixed_layout,
        skipped_results_count,
        filename_only_count,
        forced_skip_count,
        duplicate_count,
        warning_needed,
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

function collect_fdr_plots(root::AbstractString)
    plots_by_search = Dict{String, Dict{String, Vector{String}}}()
    for path in metrics_files(root)
        dataset, search = parse_dataset_search(path, root)
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

function build_html_report(
    plots_by_search::Dict{String, Dict{String, Vector{String}}},
    output_path::AbstractString,
    metrics_report::AbstractString,
)
    output_dir = dirname(output_path)
    plot_root = joinpath(output_dir, "fdr_plots")
    mkpath(output_dir)
    mkpath(plot_root)

    buffer = IOBuffer()
    println(buffer, "<!DOCTYPE html>")
    println(buffer, "<html lang=\"en\">")
    println(buffer, "<head>")
    println(buffer, "<meta charset=\"UTF-8\">")
    println(buffer, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">")
    println(buffer, "<title>Regression Metrics eFDR Plots</title>")
    println(buffer, "<style>")
    println(buffer, "body { font-family: Arial, sans-serif; margin: 24px; }")
    println(buffer, "h1 { margin-bottom: 8px; }")
    println(buffer, ".search-block { margin-bottom: 32px; }")
    println(buffer, ".dataset-block { margin-bottom: 20px; }")
    println(buffer, ".plot-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }")
    println(buffer, ".plot-card { border: 1px solid #ddd; padding: 8px; border-radius: 6px; }")
    println(buffer, ".plot-card img { width: 100%; height: auto; display: block; }")
    println(buffer, ".plot-caption { font-size: 12px; margin-top: 6px; color: #555; word-break: break-all; }")
    println(buffer, ".metrics-report { margin-bottom: 32px; }")
    println(buffer, ".metric-block { margin-bottom: 24px; }")
    println(buffer, ".metrics-table { border-collapse: collapse; width: 100%; margin-bottom: 16px; }")
    println(buffer, ".metrics-table th, .metrics-table td { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }")
    println(buffer, ".metrics-table th { background: #f4f4f4; text-align: left; }")
    println(buffer, ".metrics-table th.numeric, .metrics-table td.numeric { text-align: right; font-variant-numeric: tabular-nums; }")
    println(buffer, ".metrics-table tbody tr:nth-child(even) { background: #fafafa; }")
    println(buffer, ".metrics-table tbody tr:hover td { background: #eef6ff; }")
    println(buffer, ".metrics-table td.metric-good { background: #e7f3f5; }")
    println(buffer, ".metrics-table td.metric-bad { background: #f6e9ea; }")
    println(buffer, ".metrics-table thead th { position: sticky; top: 0; z-index: 1; }")
    println(buffer, ".metrics-table th.sortable { cursor: pointer; }")
    println(buffer, ".table-controls { margin: 6px 0 10px; display: flex; gap: 8px; align-items: center; }")
    println(buffer, ".table-controls label { font-size: 12px; color: #444; }")
    println(buffer, ".table-filter { padding: 4px 6px; font-size: 12px; min-width: 220px; }")
    println(buffer, ".table-chart { height: 520px; margin: 6px 0 12px; }")
    println(buffer, ".warning-banner { background: #fff4e5; border: 1px solid #f1b165; padding: 8px 12px; border-radius: 6px; color: #7a4b00; margin-bottom: 12px; }")
    println(buffer, "</style>")
    println(buffer, "<script src=\"https://cdn.plot.ly/plotly-2.27.0.min.js\"></script>")
    println(buffer, "</head>")
    println(buffer, "<body>")
    println(buffer, "<div class=\"metrics-report\">")
    println(buffer, metrics_report)
    println(buffer, "</div>")

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
                println(buffer, "<img src=\"", html_escape(rel_path), "\" alt=\"", html_escape(basename(plot_path)), "\">")
                println(buffer, "<div class=\"plot-caption\">", html_escape(basename(plot_path)), "</div>")
                println(buffer, "</div>")
            end
            println(buffer, "</div>")
            println(buffer, "</div>")
        end
        println(buffer, "</div>")
    end

    println(buffer, "<script>")
    println(buffer, "function parseCellValue(text) {")
    println(buffer, "  const trimmed = text.trim();")
    println(buffer, "  if (!trimmed || trimmed === \"NA\") return NaN;")
    println(buffer, "  const cleaned = trimmed.replace(/[%+,]/g, \"\");")
    println(buffer, "  const value = parseFloat(cleaned);")
    println(buffer, "  return Number.isNaN(value) ? NaN : value;")
    println(buffer, "}")
    println(buffer, "function sortTable(table, columnIndex, direction) {")
    println(buffer, "  const tbody = table.tBodies[0];")
    println(buffer, "  if (!tbody) return;")
    println(buffer, "  const rows = Array.from(tbody.rows);")
    println(buffer, "  rows.sort((rowA, rowB) => {")
    println(buffer, "    const cellA = rowA.cells[columnIndex];")
    println(buffer, "    const cellB = rowB.cells[columnIndex];")
    println(buffer, "    const textA = cellA ? cellA.textContent : \"\";")
    println(buffer, "    const textB = cellB ? cellB.textContent : \"\";")
    println(buffer, "    const numA = parseCellValue(textA);")
    println(buffer, "    const numB = parseCellValue(textB);")
    println(buffer, "    let result = 0;")
    println(buffer, "    if (!Number.isNaN(numA) && !Number.isNaN(numB)) {")
    println(buffer, "      result = numA - numB;")
    println(buffer, "    } else if (!Number.isNaN(numA)) {")
    println(buffer, "      result = 1;")
    println(buffer, "    } else if (!Number.isNaN(numB)) {")
    println(buffer, "      result = -1;")
    println(buffer, "    } else {")
    println(buffer, "      result = textA.localeCompare(textB);")
    println(buffer, "    }")
    println(buffer, "    return direction === \"asc\" ? result : -result;")
    println(buffer, "  });")
    println(buffer, "  rows.forEach(row => tbody.appendChild(row));")
    println(buffer, "  table.dataset.sortIndex = columnIndex;")
    println(buffer, "  table.dataset.sortDirection = direction;")
    println(buffer, "}")
    println(buffer, "function setupSortableTables() {")
    println(buffer, "  const tables = document.querySelectorAll(\"table.metrics-table\");")
    println(buffer, "  tables.forEach(table => {")
    println(buffer, "    const headers = table.querySelectorAll(\"thead th\");")
    println(buffer, "    headers.forEach((header, index) => {")
    println(buffer, "      header.classList.add(\"sortable\");")
    println(buffer, "      header.addEventListener(\"click\", () => {")
    println(buffer, "        const currentIndex = parseInt(table.dataset.sortIndex || \"-1\", 10);")
    println(buffer, "        const currentDirection = table.dataset.sortDirection || \"desc\";")
    println(buffer, "        const nextDirection = currentIndex === index && currentDirection === \"desc\" ? \"asc\" : \"desc\";")
    println(buffer, "        sortTable(table, index, nextDirection);")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "    const initial = table.dataset.initialSort;")
    println(buffer, "    if (initial !== undefined && initial !== \"\") {")
    println(buffer, "      sortTable(table, parseInt(initial, 10), \"desc\");")
    println(buffer, "    }")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function setupTableFilters() {")
    println(buffer, "  const inputs = document.querySelectorAll(\".table-filter\");")
    println(buffer, "  inputs.forEach(input => {")
    println(buffer, "    const tableId = input.dataset.tableId;")
    println(buffer, "    if (!tableId) return;")
    println(buffer, "    const table = document.getElementById(tableId);")
    println(buffer, "    if (!table) return;")
    println(buffer, "    input.addEventListener(\"input\", () => {")
    println(buffer, "      const query = input.value.trim().toLowerCase();")
    println(buffer, "      const rows = Array.from(table.tBodies[0].rows);")
    println(buffer, "      rows.forEach(row => {")
    println(buffer, "        const text = row.textContent.toLowerCase();")
    println(buffer, "        const dataset = (row.dataset.dataset || \"\").toLowerCase();")
    println(buffer, "        const search = (row.dataset.search || \"\").toLowerCase();")
    println(buffer, "        const haystack = `\${text} \${dataset} \${search}`;")
    println(buffer, "        row.style.display = !query || haystack.includes(query) ? \"\" : \"none\";")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function collectSeries(table, mode) {")
    println(buffer, "  const headers = Array.from(table.querySelectorAll(\"thead th\")).map(th => th.textContent.trim());")
    println(buffer, "  const versions = (table.dataset.versions || \"\").split(\"|\").filter(Boolean);")
    println(buffer, "  const rows = Array.from(table.tBodies[0].rows);")
    println(buffer, "  if (!rows.length) return [];")
    println(buffer, "  const datasetValues = rows.map(row => row.dataset.dataset || (row.cells[0] ? row.cells[0].textContent.trim() : \"\"));")
    println(buffer, "  const searchValues = rows.map(row => row.dataset.search || \"\");")
    println(buffer, "  const group = (table.dataset.metricGroup || \"\").toLowerCase();")
    println(buffer, "  const speciesIndex = headers.indexOf(\"Species\");")
    println(buffer, "  const conditionIndex = headers.indexOf(\"Condition\");")
    println(buffer, "  const speciesValues = speciesIndex >= 0 ? rows.map(row => {")
    println(buffer, "    const cell = row.cells[speciesIndex];")
    println(buffer, "    return cell ? cell.textContent.trim() : \"\";")
    println(buffer, "  }) : [];")
    println(buffer, "  const conditionValues = conditionIndex >= 0 ? rows.map(row => {")
    println(buffer, "    const cell = row.cells[conditionIndex];")
    println(buffer, "    return cell ? cell.textContent.trim() : \"\";")
    println(buffer, "  }) : [];")
    println(buffer, "  const usesCompositeXAxis = (group === \"fold_change\" || group === \"fold_change_variance\") && speciesIndex >= 0 && conditionIndex >= 0;")
    println(buffer, "  const xValues = usesCompositeXAxis ? rows.map((row, idx) => {")
    println(buffer, "    const dataset = datasetValues[idx];")
    println(buffer, "    const species = speciesValues[idx];")
    println(buffer, "    const condition = conditionValues[idx];")
    println(buffer, "    const parts = [];")
    println(buffer, "    if (dataset) parts.push(dataset);")
    println(buffer, "    if (species) parts.push(species);")
    println(buffer, "    if (condition) parts.push(condition);")
    println(buffer, "    return parts.join(\" / \") || dataset || \"\";")
    println(buffer, "  }) : datasetValues;")
    println(buffer, "  const shouldSplitBySpecies = false;")
    println(buffer, "  const columnIndexes = [];")
    println(buffer, "  const labels = [];")
    println(buffer, "  if (mode === \"raw\") {")
    println(buffer, "    versions.forEach(version => {")
    println(buffer, "      const idx = headers.indexOf(version);")
    println(buffer, "      if (idx >= 0) {")
    println(buffer, "        columnIndexes.push(idx);")
    println(buffer, "        labels.push(version);")
    println(buffer, "      }")
    println(buffer, "    });")
    println(buffer, "  } else if (mode === \"delta\") {")
    println(buffer, "    headers.forEach((header, idx) => {")
    println(buffer, "      if (header.startsWith(\"Δ \")) {")
    println(buffer, "        columnIndexes.push(idx);")
    println(buffer, "        labels.push(header);")
    println(buffer, "      }")
    println(buffer, "    });")
    println(buffer, "  } else if (mode === \"percent_delta\") {")
    println(buffer, "    headers.forEach((header, idx) => {")
    println(buffer, "      if (header.startsWith(\"% \")) {")
    println(buffer, "        columnIndexes.push(idx);")
    println(buffer, "        labels.push(header);")
    println(buffer, "      }")
    println(buffer, "    });")
    println(buffer, "  }")
    println(buffer, "  if (!columnIndexes.length) return [];")
    println(buffer, "  if (!shouldSplitBySpecies) {")
    println(buffer, "    return labels.map((label, seriesIdx) => {")
    println(buffer, "      const columnIndex = columnIndexes[seriesIdx];")
    println(buffer, "      const yValues = rows.map(row => {")
    println(buffer, "        const cell = row.cells[columnIndex];")
    println(buffer, "        const value = cell ? parseCellValue(cell.textContent) : NaN;")
    println(buffer, "        return Number.isNaN(value) ? null : value;")
    println(buffer, "      });")
    println(buffer, "      const hoverText = rows.map((row, idx) => {")
    println(buffer, "        const dataset = xValues[idx];")
    println(buffer, "        const search = searchValues[idx];")
    println(buffer, "        return search ? `\${dataset}<br>\${search}` : dataset;")
    println(buffer, "      });")
    println(buffer, "      return {")
    println(buffer, "        name: label,")
    println(buffer, "        x: xValues,")
    println(buffer, "        y: yValues,")
    println(buffer, "        text: hoverText,")
    println(buffer, "        hovertemplate: \"%{text}<br>%{y}<extra>%{name}</extra>\",")
    println(buffer, "        mode: \"lines+markers\"")
    println(buffer, "      };")
    println(buffer, "    });")
    println(buffer, "  }")
    println(buffer, "  const seenSpecies = new Set();")
    println(buffer, "  const speciesList = [];")
    println(buffer, "  speciesValues.forEach(value => {")
    println(buffer, "    if (seenSpecies.has(value)) return;")
    println(buffer, "    seenSpecies.add(value);")
    println(buffer, "    speciesList.push(value);")
    println(buffer, "  });")
    println(buffer, "  const series = [];")
    println(buffer, "  labels.forEach((label, seriesIdx) => {")
    println(buffer, "    const columnIndex = columnIndexes[seriesIdx];")
    println(buffer, "    speciesList.forEach(species => {")
    println(buffer, "      const x = [];")
    println(buffer, "      const y = [];")
    println(buffer, "      const hoverText = [];")
    println(buffer, "      rows.forEach((row, idx) => {")
    println(buffer, "        if (speciesValues[idx] !== species) return;")
    println(buffer, "        x.push(xValues[idx]);")
    println(buffer, "        const cell = row.cells[columnIndex];")
    println(buffer, "        const value = cell ? parseCellValue(cell.textContent) : NaN;")
    println(buffer, "        y.push(Number.isNaN(value) ? null : value);")
    println(buffer, "        const dataset = xValues[idx];")
    println(buffer, "        const search = searchValues[idx];")
    println(buffer, "        hoverText.push(search ? `\${dataset}<br>\${search}` : dataset);")
    println(buffer, "      });")
    println(buffer, "      const hasValue = y.some(value => value !== null && value !== undefined);")
    println(buffer, "      if (!hasValue) return;")
    println(buffer, "      const speciesLabel = species || \"Unknown\";")
    println(buffer, "      series.push({")
    println(buffer, "        name: `\${label} · \${speciesLabel}`,")
    println(buffer, "        x,")
    println(buffer, "        y,")
    println(buffer, "        text: hoverText,")
    println(buffer, "        hovertemplate: \"%{text}<br>%{y}<extra>%{name}</extra>\",")
    println(buffer, "        mode: \"lines+markers\"")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "  });")
    println(buffer, "  return series;")
    println(buffer, "}")
    println(buffer, "function choosePrimarySeries(series) {")
    println(buffer, "  if (!series.length) return null;")
    println(buffer, "  const byName = needle => series.find(trace => (trace.name || \"\").toLowerCase().includes(needle));")
    println(buffer, "  const currentVsDevelop = byName(\"current vs develop\");")
    println(buffer, "  if (currentVsDevelop) return currentVsDevelop;")
    println(buffer, "  const currentExact = series.find(trace => (trace.name || \"\").trim().toLowerCase() === \"current\");")
    println(buffer, "  if (currentExact) return currentExact;")
    println(buffer, "  return series[0];")
    println(buffer, "}")
    println(buffer, "function sortSeriesByY(series) {")
    println(buffer, "  const primary = choosePrimarySeries(series);")
    println(buffer, "  if (!primary) return series;")
    println(buffer, "  const indices = primary.y.map((value, idx) => ({")
    println(buffer, "    idx,")
    println(buffer, "    value: value === null || value === undefined || Number.isNaN(value) ? -Infinity : value")
    println(buffer, "  }));")
    println(buffer, "  indices.sort((a, b) => b.value - a.value);")
    println(buffer, "  const order = indices.map(entry => entry.idx);")
    println(buffer, "  return series.map(trace => {")
    println(buffer, "    return {")
    println(buffer, "      ...trace,")
    println(buffer, "      x: order.map(idx => trace.x[idx]),")
    println(buffer, "      y: order.map(idx => trace.y[idx]),")
    println(buffer, "      text: trace.text ? order.map(idx => trace.text[idx]) : trace.text")
    println(buffer, "    };")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function setupTableCharts() {")
    println(buffer, "  if (typeof Plotly === \"undefined\") return;")
    println(buffer, "  const chartContainers = document.querySelectorAll(\".table-chart\");")
    println(buffer, "  chartContainers.forEach(container => {")
    println(buffer, "    const tableId = container.dataset.tableId;")
    println(buffer, "    const mode = container.dataset.chartMode || \"\";")
    println(buffer, "    if (!tableId || !mode) return;")
    println(buffer, "    const table = document.getElementById(tableId);")
    println(buffer, "    if (!table) return;")
    println(buffer, "    const series = sortSeriesByY(collectSeries(table, mode));")
    println(buffer, "    if (!series.length) {")
    println(buffer, "      container.style.display = \"none\";")
    println(buffer, "      return;")
    println(buffer, "    }")
    println(buffer, "    const showZeroLine = mode !== \"raw\";")
    println(buffer, "    const group = (table.dataset.metricGroup || \"\").toLowerCase();")
    println(buffer, "    const xAxisTitle = (group === \"fold_change\" || group === \"fold_change_variance\") ? \"Dataset / Species / Condition\" : \"Dataset\";")
    println(buffer, "    const layout = {")
    println(buffer, "      margin: { l: 50, r: 20, t: 10, b: 40 },")
    println(buffer, "      height: 520,")
    println(buffer, "      xaxis: { title: xAxisTitle, automargin: true },")
    println(buffer, "      yaxis: { title: mode === \"raw\" ? \"Value\" : (mode === \"percent_delta\" ? \"Δ %\" : \"Δ\"), automargin: true },")
    println(buffer, "      legend: { orientation: \"h\", x: 1, y: 1, xanchor: \"right\", yanchor: \"top\" },")
    println(buffer, "      shapes: showZeroLine ? [{ type: \"line\", xref: \"paper\", x0: 0, x1: 1, y0: 0, y1: 0, line: { color: \"#666\", width: 1, dash: \"dash\" } }] : []")
    println(buffer, "    };")
    println(buffer, "    Plotly.newPlot(container, series, layout, { displaylogo: false, responsive: true });")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function applyValueCues() {")
    println(buffer, "  const tables = document.querySelectorAll(\"table.metrics-table\");")
    println(buffer, "  tables.forEach(table => {")
    println(buffer, "    const group = (table.dataset.metricGroup || \"\").toLowerCase();")
    println(buffer, "    const headers = Array.from(table.querySelectorAll(\"thead th\")).map(th => th.textContent.trim());")
    println(buffer, "    const rows = Array.from(table.tBodies[0]?.rows || []);")
    println(buffer, "    rows.forEach(row => {")
    println(buffer, "      Array.from(row.cells).forEach((cell, idx) => {")
    println(buffer, "        if (!cell.classList.contains(\"numeric\")) return;")
    println(buffer, "        const header = headers[idx] || \"\";")
    println(buffer, "        const value = parseCellValue(cell.textContent);")
    println(buffer, "        if (Number.isNaN(value)) return;")
    println(buffer, "        const isDelta = header.startsWith(\"Δ \");")
    println(buffer, "        const isPercent = header.startsWith(\"% \");")
    println(buffer, "        const isRaw = !isDelta && !isPercent;")
    println(buffer, "        let classification = null;")
    println(buffer, "        if (group === \"identification\" && (isDelta || isPercent)) {")
    println(buffer, "          if (value > 0) classification = \"good\";")
    println(buffer, "          else if (value < 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"runtime\" && (isDelta || isPercent)) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"cv\" && isDelta) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"fold_change_variance\" && isDelta) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"entrapment\" && isRaw) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"ftr\" && isRaw) {")
    println(buffer, "          if (value < 0.01) classification = \"good\";")
    println(buffer, "          else if (value > 0.01) classification = \"bad\";")
    println(buffer, "        }")
    println(buffer, "        if (!classification) return;")
    println(buffer, "        cell.classList.remove(\"metric-good\", \"metric-bad\");")
    println(buffer, "        cell.classList.add(classification === \"good\" ? \"metric-good\" : \"metric-bad\");")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "if (document.readyState === \"loading\") {")
    println(buffer, "  document.addEventListener(\"DOMContentLoaded\", () => {")
    println(buffer, "    setupSortableTables();")
    println(buffer, "    setupTableFilters();")
    println(buffer, "    setupTableCharts();")
    println(buffer, "    applyValueCues();")
    println(buffer, "  });")
    println(buffer, "} else {")
    println(buffer, "  setupSortableTables();")
    println(buffer, "  setupTableFilters();")
    println(buffer, "  setupTableCharts();")
    println(buffer, "  applyValueCues();")
    println(buffer, "}")
    println(buffer, "</script>")
    println(buffer, "</body>")
    println(buffer, "</html>")
    String(take!(buffer))
end

function build_metrics_report_page(metrics_report::AbstractString)
    buffer = IOBuffer()
    println(buffer, "<!DOCTYPE html>")
    println(buffer, "<html lang=\"en\">")
    println(buffer, "<head>")
    println(buffer, "<meta charset=\"UTF-8\">")
    println(buffer, "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">")
    println(buffer, "<title>Regression Metrics Report</title>")
    println(buffer, "<style>")
    println(buffer, "body { font-family: Arial, sans-serif; margin: 24px; }")
    println(buffer, ".metrics-report { margin-bottom: 32px; }")
    println(buffer, ".metric-block { margin-bottom: 24px; }")
    println(buffer, ".metrics-table { border-collapse: collapse; width: 100%; margin-bottom: 16px; }")
    println(buffer, ".metrics-table th, .metrics-table td { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }")
    println(buffer, ".metrics-table th { background: #f4f4f4; text-align: left; }")
    println(buffer, ".metrics-table th.numeric, .metrics-table td.numeric { text-align: right; font-variant-numeric: tabular-nums; }")
    println(buffer, ".metrics-table tbody tr:nth-child(even) { background: #fafafa; }")
    println(buffer, ".metrics-table tbody tr:hover td { background: #eef6ff; }")
    println(buffer, ".metrics-table td.metric-good { background: #e7f3f5; }")
    println(buffer, ".metrics-table td.metric-bad { background: #f6e9ea; }")
    println(buffer, ".metrics-table thead th { position: sticky; top: 0; z-index: 1; }")
    println(buffer, ".metrics-table th.sortable { cursor: pointer; }")
    println(buffer, ".table-controls { margin: 6px 0 10px; display: flex; gap: 8px; align-items: center; }")
    println(buffer, ".table-controls label { font-size: 12px; color: #444; }")
    println(buffer, ".table-filter { padding: 4px 6px; font-size: 12px; min-width: 220px; }")
    println(buffer, ".table-chart { height: 520px; margin: 6px 0 12px; }")
    println(buffer, ".warning-banner { background: #fff4e5; border: 1px solid #f1b165; padding: 8px 12px; border-radius: 6px; color: #7a4b00; margin-bottom: 12px; }")
    println(buffer, "</style>")
    println(buffer, "<script src=\"https://cdn.plot.ly/plotly-2.27.0.min.js\"></script>")
    println(buffer, "</head>")
    println(buffer, "<body>")
    println(buffer, "<div class=\"metrics-report\">")
    println(buffer, metrics_report)
    println(buffer, "</div>")
    println(buffer, "<script>")
    println(buffer, "function parseCellValue(text) {")
    println(buffer, "  const trimmed = text.trim();")
    println(buffer, "  if (!trimmed || trimmed === \"NA\") return NaN;")
    println(buffer, "  const cleaned = trimmed.replace(/[%+,]/g, \"\");")
    println(buffer, "  const value = parseFloat(cleaned);")
    println(buffer, "  return Number.isNaN(value) ? NaN : value;")
    println(buffer, "}")
    println(buffer, "function sortTable(table, columnIndex, direction) {")
    println(buffer, "  const tbody = table.tBodies[0];")
    println(buffer, "  if (!tbody) return;")
    println(buffer, "  const rows = Array.from(tbody.rows);")
    println(buffer, "  rows.sort((rowA, rowB) => {")
    println(buffer, "    const cellA = rowA.cells[columnIndex];")
    println(buffer, "    const cellB = rowB.cells[columnIndex];")
    println(buffer, "    const textA = cellA ? cellA.textContent : \"\";")
    println(buffer, "    const textB = cellB ? cellB.textContent : \"\";")
    println(buffer, "    const numA = parseCellValue(textA);")
    println(buffer, "    const numB = parseCellValue(textB);")
    println(buffer, "    let result = 0;")
    println(buffer, "    if (!Number.isNaN(numA) && !Number.isNaN(numB)) {")
    println(buffer, "      result = numA - numB;")
    println(buffer, "    } else if (!Number.isNaN(numA)) {")
    println(buffer, "      result = 1;")
    println(buffer, "    } else if (!Number.isNaN(numB)) {")
    println(buffer, "      result = -1;")
    println(buffer, "    } else {")
    println(buffer, "      result = textA.localeCompare(textB);")
    println(buffer, "    }")
    println(buffer, "    return direction === \"asc\" ? result : -result;")
    println(buffer, "  });")
    println(buffer, "  rows.forEach(row => tbody.appendChild(row));")
    println(buffer, "  table.dataset.sortIndex = columnIndex;")
    println(buffer, "  table.dataset.sortDirection = direction;")
    println(buffer, "}")
    println(buffer, "function setupSortableTables() {")
    println(buffer, "  const tables = document.querySelectorAll(\"table.metrics-table\");")
    println(buffer, "  tables.forEach(table => {")
    println(buffer, "    const headers = table.querySelectorAll(\"thead th\");")
    println(buffer, "    headers.forEach((header, index) => {")
    println(buffer, "      header.classList.add(\"sortable\");")
    println(buffer, "      header.addEventListener(\"click\", () => {")
    println(buffer, "        const currentIndex = parseInt(table.dataset.sortIndex || \"-1\", 10);")
    println(buffer, "        const currentDirection = table.dataset.sortDirection || \"desc\";")
    println(buffer, "        const nextDirection = currentIndex === index && currentDirection === \"desc\" ? \"asc\" : \"desc\";")
    println(buffer, "        sortTable(table, index, nextDirection);")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "    const initial = table.dataset.initialSort;")
    println(buffer, "    if (initial !== undefined && initial !== \"\") {")
    println(buffer, "      sortTable(table, parseInt(initial, 10), \"desc\");")
    println(buffer, "    }")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function setupTableFilters() {")
    println(buffer, "  const inputs = document.querySelectorAll(\".table-filter\");")
    println(buffer, "  inputs.forEach(input => {")
    println(buffer, "    const tableId = input.dataset.tableId;")
    println(buffer, "    if (!tableId) return;")
    println(buffer, "    const table = document.getElementById(tableId);")
    println(buffer, "    if (!table) return;")
    println(buffer, "    input.addEventListener(\"input\", () => {")
    println(buffer, "      const query = input.value.trim().toLowerCase();")
    println(buffer, "      const rows = Array.from(table.tBodies[0].rows);")
    println(buffer, "      rows.forEach(row => {")
    println(buffer, "        const text = row.textContent.toLowerCase();")
    println(buffer, "        const dataset = (row.dataset.dataset || \"\").toLowerCase();")
    println(buffer, "        const search = (row.dataset.search || \"\").toLowerCase();")
    println(buffer, "        const haystack = `\${text} \${dataset} \${search}`;")
    println(buffer, "        row.style.display = !query || haystack.includes(query) ? \"\" : \"none\";")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function collectSeries(table, mode) {")
    println(buffer, "  const headers = Array.from(table.querySelectorAll(\"thead th\")).map(th => th.textContent.trim());")
    println(buffer, "  const versions = (table.dataset.versions || \"\").split(\"|\").filter(Boolean);")
    println(buffer, "  const rows = Array.from(table.tBodies[0].rows);")
    println(buffer, "  if (!rows.length) return [];")
    println(buffer, "  const datasetValues = rows.map(row => row.dataset.dataset || (row.cells[0] ? row.cells[0].textContent.trim() : \"\"));")
    println(buffer, "  const searchValues = rows.map(row => row.dataset.search || \"\");")
    println(buffer, "  const group = (table.dataset.metricGroup || \"\").toLowerCase();")
    println(buffer, "  const speciesIndex = headers.indexOf(\"Species\");")
    println(buffer, "  const conditionIndex = headers.indexOf(\"Condition\");")
    println(buffer, "  const speciesValues = speciesIndex >= 0 ? rows.map(row => {")
    println(buffer, "    const cell = row.cells[speciesIndex];")
    println(buffer, "    return cell ? cell.textContent.trim() : \"\";")
    println(buffer, "  }) : [];")
    println(buffer, "  const conditionValues = conditionIndex >= 0 ? rows.map(row => {")
    println(buffer, "    const cell = row.cells[conditionIndex];")
    println(buffer, "    return cell ? cell.textContent.trim() : \"\";")
    println(buffer, "  }) : [];")
    println(buffer, "  const usesCompositeXAxis = (group === \"fold_change\" || group === \"fold_change_variance\") && speciesIndex >= 0 && conditionIndex >= 0;")
    println(buffer, "  const xValues = usesCompositeXAxis ? rows.map((row, idx) => {")
    println(buffer, "    const dataset = datasetValues[idx];")
    println(buffer, "    const species = speciesValues[idx];")
    println(buffer, "    const condition = conditionValues[idx];")
    println(buffer, "    const parts = [];")
    println(buffer, "    if (dataset) parts.push(dataset);")
    println(buffer, "    if (species) parts.push(species);")
    println(buffer, "    if (condition) parts.push(condition);")
    println(buffer, "    return parts.join(\" / \") || dataset || \"\";")
    println(buffer, "  }) : datasetValues;")
    println(buffer, "  const shouldSplitBySpecies = false;")
    println(buffer, "  const columnIndexes = [];")
    println(buffer, "  const labels = [];")
    println(buffer, "  if (mode === \"raw\") {")
    println(buffer, "    versions.forEach(version => {")
    println(buffer, "      const idx = headers.indexOf(version);")
    println(buffer, "      if (idx >= 0) {")
    println(buffer, "        columnIndexes.push(idx);")
    println(buffer, "        labels.push(version);")
    println(buffer, "      }")
    println(buffer, "    });")
    println(buffer, "  } else if (mode === \"delta\") {")
    println(buffer, "    headers.forEach((header, idx) => {")
    println(buffer, "      if (header.startsWith(\"Δ \")) {")
    println(buffer, "        columnIndexes.push(idx);")
    println(buffer, "        labels.push(header);")
    println(buffer, "      }")
    println(buffer, "    });")
    println(buffer, "  } else if (mode === \"percent_delta\") {")
    println(buffer, "    headers.forEach((header, idx) => {")
    println(buffer, "      if (header.startsWith(\"% \")) {")
    println(buffer, "        columnIndexes.push(idx);")
    println(buffer, "        labels.push(header);")
    println(buffer, "      }")
    println(buffer, "    });")
    println(buffer, "  }")
    println(buffer, "  if (!columnIndexes.length) return [];")
    println(buffer, "  if (!shouldSplitBySpecies) {")
    println(buffer, "    return labels.map((label, seriesIdx) => {")
    println(buffer, "      const columnIndex = columnIndexes[seriesIdx];")
    println(buffer, "      const yValues = rows.map(row => {")
    println(buffer, "        const cell = row.cells[columnIndex];")
    println(buffer, "        const value = cell ? parseCellValue(cell.textContent) : NaN;")
    println(buffer, "        return Number.isNaN(value) ? null : value;")
    println(buffer, "      });")
    println(buffer, "      const hoverText = rows.map((row, idx) => {")
    println(buffer, "        const dataset = xValues[idx];")
    println(buffer, "        const search = searchValues[idx];")
    println(buffer, "        return search ? `\${dataset}<br>\${search}` : dataset;")
    println(buffer, "      });")
    println(buffer, "      return {")
    println(buffer, "        name: label,")
    println(buffer, "        x: xValues,")
    println(buffer, "        y: yValues,")
    println(buffer, "        text: hoverText,")
    println(buffer, "        hovertemplate: \"%{text}<br>%{y}<extra>%{name}</extra>\",")
    println(buffer, "        mode: \"lines+markers\"")
    println(buffer, "      };")
    println(buffer, "    });")
    println(buffer, "  }")
    println(buffer, "  const seenSpecies = new Set();")
    println(buffer, "  const speciesList = [];")
    println(buffer, "  speciesValues.forEach(value => {")
    println(buffer, "    if (seenSpecies.has(value)) return;")
    println(buffer, "    seenSpecies.add(value);")
    println(buffer, "    speciesList.push(value);")
    println(buffer, "  });")
    println(buffer, "  const series = [];")
    println(buffer, "  labels.forEach((label, seriesIdx) => {")
    println(buffer, "    const columnIndex = columnIndexes[seriesIdx];")
    println(buffer, "    speciesList.forEach(species => {")
    println(buffer, "      const x = [];")
    println(buffer, "      const y = [];")
    println(buffer, "      const hoverText = [];")
    println(buffer, "      rows.forEach((row, idx) => {")
    println(buffer, "        if (speciesValues[idx] !== species) return;")
    println(buffer, "        x.push(xValues[idx]);")
    println(buffer, "        const cell = row.cells[columnIndex];")
    println(buffer, "        const value = cell ? parseCellValue(cell.textContent) : NaN;")
    println(buffer, "        y.push(Number.isNaN(value) ? null : value);")
    println(buffer, "        const dataset = xValues[idx];")
    println(buffer, "        const search = searchValues[idx];")
    println(buffer, "        hoverText.push(search ? `\${dataset}<br>\${search}` : dataset);")
    println(buffer, "      });")
    println(buffer, "      const hasValue = y.some(value => value !== null && value !== undefined);")
    println(buffer, "      if (!hasValue) return;")
    println(buffer, "      const speciesLabel = species || \"Unknown\";")
    println(buffer, "      series.push({")
    println(buffer, "        name: `\${label} · \${speciesLabel}`,")
    println(buffer, "        x,")
    println(buffer, "        y,")
    println(buffer, "        text: hoverText,")
    println(buffer, "        hovertemplate: \"%{text}<br>%{y}<extra>%{name}</extra>\",")
    println(buffer, "        mode: \"lines+markers\"")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "  });")
    println(buffer, "  return series;")
    println(buffer, "}")
    println(buffer, "function choosePrimarySeries(series) {")
    println(buffer, "  if (!series.length) return null;")
    println(buffer, "  const byName = needle => series.find(trace => (trace.name || \"\").toLowerCase().includes(needle));")
    println(buffer, "  const currentVsDevelop = byName(\"current vs develop\");")
    println(buffer, "  if (currentVsDevelop) return currentVsDevelop;")
    println(buffer, "  const currentExact = series.find(trace => (trace.name || \"\").trim().toLowerCase() === \"current\");")
    println(buffer, "  if (currentExact) return currentExact;")
    println(buffer, "  return series[0];")
    println(buffer, "}")
    println(buffer, "function sortSeriesByY(series) {")
    println(buffer, "  const primary = choosePrimarySeries(series);")
    println(buffer, "  if (!primary) return series;")
    println(buffer, "  const indices = primary.y.map((value, idx) => ({")
    println(buffer, "    idx,")
    println(buffer, "    value: value === null || value === undefined || Number.isNaN(value) ? -Infinity : value")
    println(buffer, "  }));")
    println(buffer, "  indices.sort((a, b) => b.value - a.value);")
    println(buffer, "  const order = indices.map(entry => entry.idx);")
    println(buffer, "  return series.map(trace => {")
    println(buffer, "    return {")
    println(buffer, "      ...trace,")
    println(buffer, "      x: order.map(idx => trace.x[idx]),")
    println(buffer, "      y: order.map(idx => trace.y[idx]),")
    println(buffer, "      text: trace.text ? order.map(idx => trace.text[idx]) : trace.text")
    println(buffer, "    };")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function setupTableCharts() {")
    println(buffer, "  if (typeof Plotly === \"undefined\") return;")
    println(buffer, "  const chartContainers = document.querySelectorAll(\".table-chart\");")
    println(buffer, "  chartContainers.forEach(container => {")
    println(buffer, "    const tableId = container.dataset.tableId;")
    println(buffer, "    const mode = container.dataset.chartMode || \"\";")
    println(buffer, "    if (!tableId || !mode) return;")
    println(buffer, "    const table = document.getElementById(tableId);")
    println(buffer, "    if (!table) return;")
    println(buffer, "    const series = sortSeriesByY(collectSeries(table, mode));")
    println(buffer, "    if (!series.length) {")
    println(buffer, "      container.style.display = \"none\";")
    println(buffer, "      return;")
    println(buffer, "    }")
    println(buffer, "    const showZeroLine = mode !== \"raw\";")
    println(buffer, "    const group = (table.dataset.metricGroup || \"\").toLowerCase();")
    println(buffer, "    const xAxisTitle = (group === \"fold_change\" || group === \"fold_change_variance\") ? \"Dataset / Species / Condition\" : \"Dataset\";")
    println(buffer, "    const layout = {")
    println(buffer, "      margin: { l: 50, r: 20, t: 10, b: 40 },")
    println(buffer, "      height: 520,")
    println(buffer, "      xaxis: { title: xAxisTitle, automargin: true },")
    println(buffer, "      yaxis: { title: mode === \"raw\" ? \"Value\" : (mode === \"percent_delta\" ? \"Δ %\" : \"Δ\"), automargin: true },")
    println(buffer, "      legend: { orientation: \"h\", x: 1, y: 1, xanchor: \"right\", yanchor: \"top\" },")
    println(buffer, "      shapes: showZeroLine ? [{ type: \"line\", xref: \"paper\", x0: 0, x1: 1, y0: 0, y1: 0, line: { color: \"#666\", width: 1, dash: \"dash\" } }] : []")
    println(buffer, "    };")
    println(buffer, "    Plotly.newPlot(container, series, layout, { displaylogo: false, responsive: true });")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "function applyValueCues() {")
    println(buffer, "  const tables = document.querySelectorAll(\"table.metrics-table\");")
    println(buffer, "  tables.forEach(table => {")
    println(buffer, "    const group = (table.dataset.metricGroup || \"\").toLowerCase();")
    println(buffer, "    const headers = Array.from(table.querySelectorAll(\"thead th\")).map(th => th.textContent.trim());")
    println(buffer, "    const rows = Array.from(table.tBodies[0]?.rows || []);")
    println(buffer, "    rows.forEach(row => {")
    println(buffer, "      Array.from(row.cells).forEach((cell, idx) => {")
    println(buffer, "        if (!cell.classList.contains(\"numeric\")) return;")
    println(buffer, "        const header = headers[idx] || \"\";")
    println(buffer, "        const value = parseCellValue(cell.textContent);")
    println(buffer, "        if (Number.isNaN(value)) return;")
    println(buffer, "        const isDelta = header.startsWith(\"Δ \");")
    println(buffer, "        const isPercent = header.startsWith(\"% \");")
    println(buffer, "        const isRaw = !isDelta && !isPercent;")
    println(buffer, "        let classification = null;")
    println(buffer, "        if (group === \"identification\" && (isDelta || isPercent)) {")
    println(buffer, "          if (value > 0) classification = \"good\";")
    println(buffer, "          else if (value < 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"runtime\" && (isDelta || isPercent)) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"cv\" && isDelta) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"fold_change_variance\" && isDelta) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"entrapment\" && isRaw) {")
    println(buffer, "          if (value < 0) classification = \"good\";")
    println(buffer, "          else if (value > 0) classification = \"bad\";")
    println(buffer, "        } else if (group === \"ftr\" && isRaw) {")
    println(buffer, "          if (value < 0.01) classification = \"good\";")
    println(buffer, "          else if (value > 0.01) classification = \"bad\";")
    println(buffer, "        }")
    println(buffer, "        if (!classification) return;")
    println(buffer, "        cell.classList.remove(\"metric-good\", \"metric-bad\");")
    println(buffer, "        cell.classList.add(classification === \"good\" ? \"metric-good\" : \"metric-bad\");")
    println(buffer, "      });")
    println(buffer, "    });")
    println(buffer, "  });")
    println(buffer, "}")
    println(buffer, "if (document.readyState === \"loading\") {")
    println(buffer, "  document.addEventListener(\"DOMContentLoaded\", () => {")
    println(buffer, "    setupSortableTables();")
    println(buffer, "    setupTableFilters();")
    println(buffer, "    setupTableCharts();")
    println(buffer, "    applyValueCues();")
    println(buffer, "  });")
    println(buffer, "} else {")
    println(buffer, "  setupSortableTables();")
    println(buffer, "  setupTableFilters();")
    println(buffer, "  setupTableCharts();")
    println(buffer, "  applyValueCues();")
    println(buffer, "}")
    println(buffer, "</script>")
    println(buffer, "</body>")
    println(buffer, "</html>")
    String(take!(buffer))
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

function layout_summary_line(layout_by_version::Dict{String, String}, versions::Vector{String})
    entries = String[]
    for version in versions
        layout = get(layout_by_version, version, "unknown")
        push!(entries, string(version, ": ", layout))
    end
    join(entries, " · ")
end

function build_report(
    version_data::AbstractDict{String, Any},
    versions::Vector{String},
    layout_by_version::Dict{String, String},
    warning_banner::AbstractString,
)
    buffer = IOBuffer()
    table_counter = Ref(0)
    println(buffer, "<div class=\"metric-block\">")
    println(buffer, "<h2>Regression Metrics Report</h2>")
    if !isempty(warning_banner)
        println(buffer, "<div class=\"warning-banner\">", html_escape(warning_banner), "</div>")
    end
    println(
        buffer,
        "<p><strong>Versions:</strong> ",
        html_escape(join(versions, ", ")),
        "</p>",
    )
    println(
        buffer,
        "<p><strong>Detected metrics layout:</strong> ",
        html_escape(layout_summary_line(layout_by_version, versions)),
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
    parse_check = truthy_env(get(ENV, "PIONEER_REPORT_PARSE_CHECK", "false"))

    if parse_check
        run_parse_check()
        return
    end

    if !isempty(metrics_root)
        isempty(release_root) && (release_root = joinpath(metrics_root, "release"))
        isempty(develop_root) && (develop_root = joinpath(metrics_root, "develop"))
        isempty(current_root) && (current_root = joinpath(metrics_root, "current"))
    end

    plots_root = get(ENV, "PIONEER_FDR_PLOTS_ROOT", current_root)

    isempty(release_root) && error("PIONEER_METRICS_RELEASE_ROOT not set")
    isempty(develop_root) && error("PIONEER_METRICS_DEVELOP_ROOT not set")
    isempty(current_root) && error("PIONEER_METRICS_CURRENT_ROOT not set")
    isempty(output_path) && error("PIONEER_REPORT_OUTPUT not set")

    versions = sorted_release_versions(release_root)
    push!(versions, "develop")
    push!(versions, "current")

    version_data = Dict{String, Any}()
    layout_by_version = Dict{String, String}()
    warning_needed = false
    for version in versions
        if version == "develop"
            summary = collect_metrics(develop_root)
            version_data[version] = summary.results
            layout_by_version[version] = summary.layout
            warning_needed |= summary.warning_needed
            @info "Collected metrics" version = version root = develop_root count = summary.metrics_count layout = summary.layout scan_root = summary.scan_root mixed_layout = summary.mixed_layout
        elseif version == "current"
            summary = collect_metrics(current_root)
            version_data[version] = summary.results
            layout_by_version[version] = summary.layout
            warning_needed |= summary.warning_needed
            @info "Collected metrics" version = version root = current_root count = summary.metrics_count layout = summary.layout scan_root = summary.scan_root mixed_layout = summary.mixed_layout
        else
            version_root = joinpath(release_root, version)
            summary = collect_metrics(version_root)
            version_data[version] = summary.results
            layout_by_version[version] = summary.layout
            warning_needed |= summary.warning_needed
            @info "Collected metrics" version = version root = version_root count = summary.metrics_count layout = summary.layout scan_root = summary.scan_root mixed_layout = summary.mixed_layout
        end
    end

    banner = warning_needed ? "Parsed metrics paths required layout adjustments; verify the metrics root and filename conventions." : ""
    report = build_report(version_data, versions, layout_by_version, banner)
    report_page = build_metrics_report_page(report)

    if isempty(html_output_path)
        base, _ = splitext(output_path)
        html_output_path = base * ".html"
    end

    open(output_path, "w") do io
        write(io, report_page)
    end

    if isdir(plots_root)
        plots_by_search = collect_fdr_plots(plots_root)
        html_report = build_html_report(plots_by_search, html_output_path, report)
        open(html_output_path, "w") do io
            write(io, html_report)
        end
    else
        @warn "Plots root not found; skipping HTML report" plots_root=plots_root
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
