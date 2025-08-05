library(arrow)
library(tidyverse)
library(magrittr)
library(duckplyr)

data <- 
  arrow::open_dataset("s3://mco-mesonet/air-quality/", 
                      format = "parquet",
                      factory_options = list(exclude_invalid_files = TRUE)) %>%
  dplyr::filter(station == "mcolubre") %>%
  dplyr::collect()


db_exec("INSTALL httpfs")
db_exec("LOAD httpfs")

read_parquet_duckdb("s3://mco-mesonet/air-quality/*/*/*.parquet",
                    options = list(union_by_name="True",
                                   hive_partitioning="True")) %>%
  # dplyr::filter(station == "mcolubre") %>%
  dplyr::select(station, time_stamp, pm2.5_atm_a, pm2.5_atm_b) %>%
  # dplyr::summarise(dplyr::across(pm2.5_atm_a:pm2.5_atm_b, mean), 
  #                  .by = station) %>%
  dplyr::collect()

