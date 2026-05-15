#!/usr/bin/env julia

include(joinpath(@__DIR__, "regression_metrics.jl"))

function write_runtime_report(contents::AbstractString)
    path = tempname()
    open(path, "w") do io
        write(io, contents)
    end
    path
end

function run_runtime_metrics_tests()
    legacy_report = write_runtime_report("""
    Pioneer Search Log

    Runtime Summary:
    Total Runtime: 15.25 minutes
    """)
    @assert runtime_minutes_from_report(legacy_report) == 15.25

    current_report = write_runtime_report("""
    Pioneer Search Log

    Runtime Summary:
    ------------------------------------------------------------------------------------------------------
    Total Runtime: 2.26 min (135.67 s)
    Per Raw File:  45.22 s
    """)
    @assert runtime_minutes_from_report(current_report) == 2.26
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_runtime_metrics_tests()
    println("runtime metrics tests passed.")
end
