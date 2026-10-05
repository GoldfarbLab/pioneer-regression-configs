#!/usr/bin/env julia

using Test

# Load only the filesystem helpers so this test does not need metrics dependencies.
const CLEANUP_HELPERS = Set([
    :elapsed_seconds, :output_metrics_path, :is_log_file, :cleanup_entrapment_dir,
    :safe_rm, :safe_mv, :cleanup_results_dir, :archive_results,
])
include(joinpath(@__DIR__, "regression_metrics.jl")) do expr
    if expr isa Expr && expr.head in (:function, :(=))
        signature = expr.args[1]
        if signature isa Expr && signature.head == :call && signature.args[1] in CLEANUP_HELPERS
            return expr
        end
    end
    nothing
end

@testset "Search summaries survive cleanup and archiving" begin
    mktempdir() do root
        archive_root = joinpath(root, "archive")
        for search_name in ("search", "search_entrap", "without_summary")
            results_dir = joinpath(root, search_name)
            mkpath(results_dir)
            metrics_path = output_metrics_path(results_dir, "dataset", search_name)
            write(metrics_path, "{}")
            write(joinpath(results_dir, "search.log"), "log")
            write(joinpath(results_dir, "intermediate.arrow"), "discard")
            has_summary = search_name != "without_summary"
            summary_contents = "search\tcount\n$(search_name)\t42\n"
            summary_path = joinpath(results_dir, "summary.tsv")
            has_summary && write(summary_path, summary_contents)

            cleanup_results_dir(results_dir, metrics_path)

            @test isfile(metrics_path)
            @test isfile(joinpath(results_dir, "search.log"))
            @test !ispath(joinpath(results_dir, "intermediate.arrow"))
            if has_summary
                @test read(summary_path, String) == summary_contents
            end

            archive_results(results_dir, metrics_path;
                archive_root = archive_root, dataset_name = "dataset", search_name = search_name)

            search_target = joinpath(archive_root, "results", "dataset", search_name)
            @test isfile(joinpath(dirname(search_target), basename(metrics_path)))
            @test isfile(joinpath(search_target, "search.log"))
            if has_summary
                @test read(joinpath(search_target, "summary.tsv"), String) == summary_contents
            else
                @test !ispath(joinpath(search_target, "summary.tsv"))
            end
        end
    end
end
