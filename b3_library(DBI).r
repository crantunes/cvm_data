library(DBI)
library(RPostgres)

con <- dbConnect(
  Postgres(),
  dbname   = "cvm_data",
  host     = "localhost",
  user     = "pesquisador",  # ajuste
  password = "pesquisador"   # ajuste
)

dbWriteTable(
  con,
  "b3_equities",
  dados_equities,
  append = TRUE,
  row.names = FALSE
)
