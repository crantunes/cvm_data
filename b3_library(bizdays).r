library(rb3)
library(bizdays)
library(dplyr)
library(purrr)

# 1) cria calendário B3 só com fins de semana como não úteis
bizdays::create.calendar(
  name      = "B3",
  holidays  = character(0),             # sem lista explícita de feriados
  weekdays  = c("saturday", "sunday")
)

# 2) gera sequência de dias úteis de 2010 até hoje
datas <- bizdays::bizseq("2010-01-01", Sys.Date(), "B3")

# 3) função para baixar um dia
baixar_dia <- function(d) {
  meta <- cotahist_get(d, "daily")      # baixa/cacheia COTAHIST_DDMMYYYY
  cotahist_equity_get(meta)            # tibble com todas as ações do dia
}

# 4) aplica em todos os dias, ignorando erros (feriados, etc.)
dados_equities <- purrr::map_dfr(
  datas,
  ~ tryCatch(baixar_dia(.x), error = function(e) NULL)
)
