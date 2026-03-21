# =========================================================
# 0. Pacotes
# =========================================================
library(rb3)
library(dplyr)
library(DBI)
library(RPostgres)
library(dotenv)
library(lubridate)

# =========================================================
# 1. Conexão
# =========================================================
dotenv::load_dot_env("d:/VSCode/cvm_data/.env")

con <- dbConnect(
  Postgres(),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  dbname   = Sys.getenv("DB_NAME"),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)
cat("Conexão OK.\n")

# =========================================================
# 2. Download COTAHIST anual 2010 a 2013
# =========================================================
cat("=== ETAPA 1: Download COTAHIST anual 2010-2013 ===\n")

fetch_marketdata("b3-cotahist-yearly", year = 2010:2013)

cat("Download concluido.\n")

# =========================================================
# 3. Ler dados de equity filtrado em 2010-2013
# =========================================================
cat("=== ETAPA 2: Lendo equity do cache ===\n")

dados_yearly <- cotahist_get("yearly") |>
  filter(refdate >= as.Date("2010-01-01") &
         refdate <= as.Date("2013-12-31")) |>
  cotahist_filter_equity() |>
  collect()

cat(sprintf("Linhas lidas: %d\n", nrow(dados_yearly)))

if (nrow(dados_yearly) == 0) {
  message("Nenhum dado retornado.")
  dbDisconnect(con)
  stop("Nada a gravar.", call. = FALSE)
}

# =========================================================
# 4. Ajustar tipos
# =========================================================
dados_yearly <- dados_yearly |>
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
# 5. Remover registros 2010-2013 se existirem (evitar dups)
# =========================================================
cat("Removendo registros anteriores de 2010-2013 (se existiam)...\n")

dbExecute(con, "DELETE FROM b3_equities
                WHERE refdate BETWEEN '2010-01-01' AND '2013-12-31'")

# =========================================================
# 6. Inserir no banco
# =========================================================
cat("=== ETAPA 3: Gravando no banco ===\n")

dbWriteTable(
  con,
  "b3_equities",
  dados_yearly,
  append    = TRUE,
  row.names = FALSE
)

cat(sprintf("Inseridas %d linhas (2010-2013) na tabela b3_equities.\n",
            nrow(dados_yearly)))

# =========================================================
# 7. Verificacao final por ano
# =========================================================
cat("\n=== Cobertura final por ano na b3_equities ===\n")

res <- dbGetQuery(con, "
  SELECT
    EXTRACT(YEAR FROM refdate) AS ano,
    COUNT(DISTINCT refdate)    AS pregoes,
    COUNT(DISTINCT symbol)     AS tickers,
    COUNT(*)                   AS linhas
  FROM b3_equities
  GROUP BY 1
  ORDER BY 1
")
print(res)

# =========================================================
# 8. Criar indices para performance
# =========================================================
cat("\n=== Criando indices ===\n")

dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_b3_equities_symbol_refdate
                ON b3_equities (symbol, refdate)")

dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_b3_equities_refdate
                ON b3_equities (refdate)")

cat("Indices criados.\n")

dbDisconnect(con)
cat("Conexao encerrada. Script finalizado com sucesso.\n")
