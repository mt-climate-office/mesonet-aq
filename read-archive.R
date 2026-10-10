# Read the Montana Mesonet PurpleAir archive from the MCO data CDN.
#
# The bucket is private; everything is served over HTTPS from
# https://data2.climate.umt.edu/mesonet/air-quality/. HTTPS has no directory
# listing, so file lists come from manifest.json. Every file has the same
# fixed schema, so multi-file reads need no type fixes.

library(jsonlite)
library(duckdb) # DuckDB >= 1.1; httpfs autoloads for https:// paths

base <- "https://data2.climate.umt.edu/mesonet/air-quality"
manifest <- fromJSON(file.path(base, "manifest.json"))

con <- dbConnect(duckdb())

# ── Hourly summaries (EPA-corrected PM2.5, NowCast, AQI): one file per station-year
hourly_urls <- with(manifest$files$hourly, file.path(base, path[station == "acebirne"]))
hourly <- dbGetQuery(con, sprintf(
  "SELECT * FROM read_parquet([%s]) ORDER BY time_stamp",
  paste0("'", hourly_urls, "'", collapse = ", ")
))

# ── Raw ~2-minute A/B readings: one file per station-month
raw_files <- subset(manifest$files$raw, station == "acebirne" & year == 2025 & month == 9)
raw <- dbGetQuery(con, sprintf(
  "SELECT * FROM read_parquet([%s]) WHERE time_stamp >= '2025-09-07' AND time_stamp < '2025-09-08'",
  paste0("'", file.path(base, raw_files$path), "'", collapse = ", ")
))

# Latest reading + NowCast AQI for every station
latest <- fromJSON(file.path(base, "latest/latest.json"))$stations

dbDisconnect(con, shutdown = TRUE)
