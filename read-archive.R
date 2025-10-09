library(arrow)
library(tidyverse)
library(magrittr)

## Some of the sensors produce missing data, which screws up parquet reads.
## This function reads everything then casts problematic columns as strings.

# Step 1: Open the dataset
ds <- 
  open_dataset("s3://mco-mesonet/air-quality")

# Step 2: Inspect column types
types <- 
  ds$schema$fields %>%
  magrittr::set_names(.,
                      purrr::map_chr(., ~ .x$name))

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
ds_fixed %>%
  filter(station == "mcolubre",
         time_stamp >= lubridate::as_datetime("2025-10-07"),
         time_stamp < lubridate::as_datetime("2025-10-08")) %>%
  collect()


