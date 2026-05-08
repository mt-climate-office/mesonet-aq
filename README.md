# mesonet-aq

Automated archiving of PurpleAir sensor data for the Montana Climate Office Mesonet network.

## Overview

This repository orchestrates the retrieval, processing, and archiving of air quality data from PurpleAir sensors
deployed across Montana. Sensor deployment metadata is managed in Airtable, and the workflow is automated via GitHub
Actions. Processed data is stored in AWS S3 as partitioned Parquet files for efficient access and analysis.

## Workflow

1. **Sensor Metadata Retrieval**

    - Connects to Airtable to fetch sensor deployment information (Deployment ID, Registration ID, Station ID,
      Deployment Date).

2. **Data Fetching**

    - For each sensor and each day since deployment, queries the PurpleAir API for historical air quality data.
    - Requests fields such as humidity, temperature, pressure, VOC, PM1.0, PM2.5, PM10.0, scattering coefficient,
      deciviews, visual range, and particle counts.

3. **Data Processing**

    - Timestamps and annotates data with station and sensor IDs.
    - Cleans and sorts data by timestamp.

4. **Archiving**

    - Uploads data to AWS S3, partitioned by station and date.
    - Detects existing partitions to avoid redundant uploads.
    - Stores data in Parquet format for downstream analysis.

5. **Automation**
    - The entire process is orchestrated via GitHub Actions, enabling scheduled and reproducible data archiving.

## Web Explorer

A static map-based explorer lives in [`docs/`](docs/). It is built to be served
by GitHub Pages (Settings → Pages → branch `main`, folder `/docs`) and uses:

-   **MapLibre GL** for the station map
-   **DuckDB-WASM** to read the partitioned Parquet directly from S3 over HTTPS
-   **DataTables** for the tabular view, **Chart.js** for the time-series

It reads `docs/manifest.json` — a list of stations and the dates we have data
for — which is regenerated at the end of every nightly action run from the
S3 listing joined with the [Montana Mesonet station API](https://mesonet.climate.umt.edu/api/v2/stations/?type=json).

### Public read access (required for the explorer)

The explorer talks to S3 directly from the browser, so the `air-quality/`
prefix needs to allow anonymous `GetObject`. Listing stays private. Apply
this bucket policy on `mco-mesonet`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "PublicReadAirQuality",
    "Effect": "Allow",
    "Principal": "*",
    "Action": "s3:GetObject",
    "Resource": "arn:aws:s3:::mco-mesonet/air-quality/*"
  }]
}
```

CORS on the bucket is already configured (`Allow-Origin: *`, methods `GET, HEAD`).
Until this policy is applied the explorer's status bar will show a clear
"S3 returned 403" message — the page itself still loads.

---

## S3 Data Archive and Partitioning

All processed air quality data are archived in the following AWS S3 bucket:

-   **Bucket:** `mco-mesonet`
-   **Prefix/Folder:** `air-quality`

### Partitioning Scheme

Data are stored in Parquet format and partitioned by **station** and **date** to optimize query performance and
organization. The partitioning scheme follows this structure:

`s3://mco-mesonet/air-quality/station=<STATION_ID>/date=<YYYY-MM-DD>/<STATION_ID>_<YYYY-MM-DD>.parquet`

-   `<STATION_ID>`: The unique identifier for each sensor station.
-   `<YYYY-MM-DD>`: The date for which the data were collected.

**Example path:**

`s3://mco-mesonet/air-quality/station=mcolubre/date=2025-10-07/mcolubre_2025-10-07.parquet`

This structure allows efficient server-side filtering and retrieval of data for specific stations and dates using tools
like https://arrow.apache.org/ and https://dplyr.tidyverse.org/ in R or Python.

---

## Example: Accessing Air Quality Data in R

```r
library(arrow)
library(tidyverse)

## Some of the sensors produce missing data, which screws up parquet reads.
## This function reads everything then casts problematic columns as strings.

# Step 1: Open the dataset
ds <-
  open_dataset("s3://mco-mesonet/air-quality")

# Step 2: Inspect column types
types <-
  ds$schema$fields
names(types) <- purrr::map_chr(types, ~ .x$name)

# Step 3: Identify fields with null or problematic types
# This is a simplified heuristic: you may need to inspect actual files for full accuracy
problematic_fields <-
  names(types)[map_lgl(types, ~ inherits(.x$type, "Null"))]

# Step 4: Build a schema that sets only problematic fields to string
custom_schema <- schema(!!!set_names(
  map(names(types),
      function(name) {
    if (name %in% problematic_fields) string() else types[[name]]$type
  }),
  names(types)
))

# Step 5: Re-open with custom schema
ds_fixed <-
  open_dataset("s3://mco-mesonet/air-quality", schema = custom_schema)

# Now, you can query server side and then collect
ds_fixed |>
  filter(station == "mcolubre",
         time_stamp >= lubridate::as_datetime("2025-10-07"),
         time_stamp < lubridate::as_datetime("2025-10-08")) |>
  collect()

```

## Requirements

-   Python 3.8+
-   Packages: `requests`, `pandas`, `boto3`, `s3fs`, `pyarrow`
-   Environment variables:
    -   `PURPLEAIR_API_KEY`
    -   `AIRTABLE_TOKEN`

## Deployment

-   **GitHub Actions:**  
    The repository includes a workflow file to automate execution of `archive-purpleair.py` on a schedule.
-   **AWS Credentials:**  
    The script uses AWS S3 for data storage. Credentials must be configured for GitHub Actions or local execution.

## License

MIT License

## Contact

For questions or contributions, please contact the Montana Climate Office or open an issue on GitHub.
