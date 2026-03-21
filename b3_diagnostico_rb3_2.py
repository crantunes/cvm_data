library(rb3)
library(dplyr)

df_teste <- cotahist_get("daily") |>
  cotahist_filter_equity() |>
  head(5) |>
  collect()

glimpse(df_teste)
