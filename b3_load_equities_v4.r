# =========================================================
# 0. Pacotes
# =========================================================
install_if_missing <- function(pkgs) {
  novos <- pkgs[!(pkgs %in% installed.packages()[, "Package"])]
  if (length(novos) > 0) install.packages(novos)
}

install_if_missing(c("rb3", "bizdays", "dplyr", "DBI",
                     "RPostgres", "dotenv", "lubridate",
                     "arrow", "duckdb"))

library(rb3)
library(bizdays)
library(dplyr)
library(DBI)
library(RPostgres)
library(dotenv)
library(lubridate)

# =========================================================
# 1. Carregar variáveis de ambiente (.env)
# =========================================================
dotenv::load_dot_env("d:/VSCode/cvm_data/.env")

db_host     <- Sys.getenv("DB_HOST")
db_port_str <- Sys.getenv("DB_PORT")
db_db       <- Sys.getenv("DB_NAME")
db_user     <- Sys.getenv("DB_USER")
db_pass     <- Sys.getenv("DB_PASSWORD")

if (db_port_str == "") stop("DB_PORT não definido no .env")
db_port <- as.integer(db_port_str)
if (is.na(db_port)) stop(paste("DB_PORT inválido:", db_port_str))

cat("Conectando em:", db_host, ":", db_port, "/", db_db, "\n")

# =========================================================
# 2. Conexão com o banco
# =========================================================
con <- dbConnect(
  Postgres(),
  host     = db_host,
  port     = db_port,
  dbname   = db_db,
  user     = db_user,
  password = db_pass
)
cat("Conexão OK.\n")

# =========================================================
# 3. Descobrir intervalo de datas a carregar
# =========================================================
ultima_data <- dbGetQuery(con, "SELECT max(refdate) AS dt FROM b3_equities")$dt

if (is.na(ultima_data)) {
  data_ini <- as.Date("2010-01-01")   # <-- confirmar que está 2010-01-01
  cat("Tabela vazia. Carregando desde 2010-01-01.\n")
}


data_fim <- Sys.Date() - 1   # D-1 (ontem, já fechado)

if (data_ini > data_fim) {
  message("Tabela já está atualizada.")
  dbDisconnect(con)
  stop("Nada a fazer.", call. = FALSE)
}

# =========================================================
# 4. Calendário B3 construído manualmente
#    Feriados nacionais B3 de 2010 a 2026
#    Em feriados não publicados, o tryCatch trata o erro
# =========================================================
feriados_b3 <- as.Date(c(
  # 2010
  "2010-01-01","2010-04-02","2010-04-21","2010-05-01",
  "2010-06-03","2010-09-07","2010-10-12","2010-11-02",
  "2010-11-15","2010-12-24","2010-12-25","2010-12-31",
  # 2011
  "2011-01-01","2011-03-07","2011-03-08","2011-04-21",
  "2011-04-22","2011-05-01","2011-06-23","2011-09-07",
  "2011-10-12","2011-11-02","2011-11-15","2011-12-25",
  "2011-12-30",
  # 2012
  "2012-01-01","2012-02-20","2012-02-21","2012-04-06",
  "2012-04-21","2012-05-01","2012-06-07","2012-09-07",
  "2012-10-12","2012-11-02","2012-11-15","2012-12-25",
  "2012-12-31",
  # 2013
  "2013-01-01","2013-02-11","2013-02-12","2013-03-29",
  "2013-04-21","2013-05-01","2013-05-30","2013-09-07",
  "2013-10-12","2013-11-02","2013-11-15","2013-12-25",
  "2013-12-31",
  # 2014
  "2014-01-01","2014-03-03","2014-03-04","2014-04-18",
  "2014-04-21","2014-05-01","2014-06-19","2014-09-07",
  "2014-10-12","2014-11-02","2014-11-15","2014-12-25",
  "2014-12-31",
  # 2015
  "2015-01-01","2015-02-16","2015-02-17","2015-04-03",
  "2015-04-21","2015-05-01","2015-06-04","2015-09-07",
  "2015-10-12","2015-11-02","2015-11-15","2015-12-25",
  "2015-12-31",
  # 2016
  "2016-01-01","2016-02-08","2016-02-09","2016-03-25",
  "2016-04-21","2016-05-01","2016-05-26","2016-09-07",
  "2016-10-12","2016-11-02","2016-11-15","2016-12-25",
  "2016-12-30",
  # 2017
  "2017-01-01","2017-02-27","2017-02-28","2017-04-14",
  "2017-04-21","2017-05-01","2017-06-15","2017-09-07",
  "2017-10-12","2017-11-02","2017-11-15","2017-12-25",
  "2017-12-29",
  # 2018
  "2018-01-01","2018-02-12","2018-02-13","2018-03-30",
  "2018-04-21","2018-05-01","2018-05-31","2018-09-07",
  "2018-10-12","2018-11-02","2018-11-15","2018-11-20",
  "2018-12-25","2018-12-31",
  # 2019
  "2019-01-01","2019-03-04","2019-03-05","2019-04-19",
  "2019-04-21","2019-05-01","2019-06-20","2019-09-07",
  "2019-10-12","2019-11-02","2019-11-15","2019-11-20",
  "2019-12-25","2019-12-31",
  # 2020
  "2020-01-01","2020-02-24","2020-02-25","2020-04-10",
  "2020-04-21","2020-05-01","2020-06-11","2020-09-07",
  "2020-10-12","2020-11-02","2020-11-15","2020-11-20",
  "2020-12-25","2020-12-31",
  # 2021
  "2021-01-01","2021-02-15","2021-02-16","2021-04-02",
  "2021-04-21","2021-05-01","2021-06-03","2021-09-07",
  "2021-10-12","2021-11-02","2021-11-15","2021-11-19",
  "2021-12-25","2021-12-31",
  # 2022
  "2022-01-01","2022-02-28","2022-03-01","2022-04-15",
  "2022-04-21","2022-05-01","2022-06-16","2022-09-07",
  "2022-10-12","2022-11-02","2022-11-15","2022-11-20",
  "2022-12-25","2022-12-30",
  # 2023
  "2023-01-01","2023-02-20","2023-02-21","2023-04-07",
  "2023-04-21","2023-05-01","2023-06-08","2023-09-07",
  "2023-10-12","2023-11-02","2023-11-15","2023-11-20",
  "2023-12-25","2023-12-29",
  # 2024
  "2024-01-01","2024-02-12","2024-02-13","2024-03-29",
  "2024-04-21","2024-05-01","2024-05-30","2024-09-07",
  "2024-10-12","2024-11-02","2024-11-15","2024-11-20",
  "2024-12-25","2024-12-31",
  # 2025
  "2025-01-01","2025-03-03","2025-03-04","2025-04-18",
  "2025-04-21","2025-05-01","2025-06-19","2025-09-07",
  "2025-10-12","2025-11-02","2025-11-15","2025-11-20",
  "2025-12-25","2025-12-31",
  # 2026
  "2026-01-01","2026-02-16","2026-02-17","2026-04-03",
  "2026-04-21","2026-05-01","2026-06-04","2026-09-07",
  "2026-10-12","2026-11-02","2026-11-20","2026-12-24",
  "2026-12-25","2026-12-31"
))

bizdays::create.calendar(
  name     = "B3_custom",
  holidays = feriados_b3,
  weekdays = c("saturday", "sunday"),
  start.date = as.Date("2009-12-31"),
  end.date   = as.Date("2027-01-01")
)

datas <- bizdays::bizseq(data_ini, data_fim, "B3_custom")

cat(sprintf("Total de dias úteis a processar: %d (%s até %s)\n",
            length(datas),
            as.character(min(datas)),
            as.character(max(datas))))

# =========================================================
# 5. Download + indexação no cache via fetch_marketdata()
# =========================================================
cat("=== ETAPA 1: Download COTAHIST via fetch_marketdata ===\n")

tryCatch({
  fetch_marketdata("b3-cotahist-daily", refdate = datas)
  cat("Download concluído.\n")
}, error = function(e) {
  cat("Erro no download:", e$message, "\n")
})

# =========================================================
# 6. Ler dados de equity do cache e filtrar período
# =========================================================
cat("=== ETAPA 2: Lendo equity do cache rb3 ===\n")

dados_equities <- cotahist_get("daily") |>
  filter(refdate >= data_ini & refdate <= data_fim) |>
  cotahist_filter_equity() |>
  collect()

cat(sprintf("Linhas lidas: %d\n", nrow(dados_equities)))

if (nrow(dados_equities) == 0) {
  message("Nenhum dado retornado. Encerrando sem gravar no banco.")
  dbDisconnect(con)
  stop("Nada a gravar.", call. = FALSE)
}

# =========================================================
# 7. Ajustar tipos compatíveis com a tabela b3_equities
# =========================================================
dados_equities <- dados_equities |>
  mutate(
    year                               = as.integer(year),
    refdate                            = as.Date(refdate),
    bdi_code                           = as.integer(bdi_code),
    symbol                             = as.character(symbol),
    instrument_market                  = as.integer(instrument_market),
    corporation_name                   = as.character(corporation_name),
    specification_code                 = as.character(specification_code),
    days_to_settlement                 = as.integer(days_to_settlement),
    trading_currency                   = as.character(trading_currency),
    open                               = as.numeric(open),
    high                               = as.numeric(high),
    low                                = as.numeric(low),
    average                            = as.numeric(average),
    close                              = as.numeric(close),
    best_bid                           = as.numeric(best_bid),
    best_ask                           = as.numeric(best_ask),
    trade_quantity                     = as.integer(trade_quantity),
    traded_contracts                   = as.integer(traded_contracts),
    volume                             = as.numeric(volume),
    strike_price                       = as.numeric(strike_price),
    strike_price_adjustment_indicator  = as.character(strike_price_adjustment_indicator),
    maturity_date                      = as.Date(maturity_date),
    allocation_lot_size                = as.integer(allocation_lot_size),
    strike_price_in_points             = as.numeric(strike_price_in_points),
    isin                               = as.character(isin),
    distribution_id                    = as.integer(distribution_id)
  )

# =========================================================
# 8. Gravar na tabela b3_equities
# =========================================================
cat("=== ETAPA 3: Gravando no banco ===\n")

dbWriteTable(
  con,
  "b3_equities",
  dados_equities,
  append    = TRUE,
  row.names = FALSE
)

cat(sprintf("Inseridas %d linhas na tabela b3_equities.\n", nrow(dados_equities)))

dbDisconnect(con)
cat("Conexão encerrada. Script finalizado com sucesso.\n")
