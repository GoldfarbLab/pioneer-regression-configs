# Three-proteome input filenames

`three_proteome_input_files.json` records the `.arrow` filenames listed on
2026-09-24 in `/storage3/fs1/d.goldfarb/Active/Automation/Pioneer/data/<dataset>`
(accessed through the `/Volumes/d.goldfarb` mount). Tests derive Pioneer output
column names by removing only the final extension, independently of the designs.

The Olsen Exploris design already excluded the `E5H50Y45` replicate 3. That
selection is preserved; the inventory includes all 24 source files. The other
four designs include every source file, for 62 configured runs in total.
