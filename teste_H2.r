# ------------------------------------------------------------
# SCRIPT: TESTE DA H2 (PARADOXO DA JURISDIÇÃO)
# OBJETIVO: Parear IFRS/CPC e calcular o CRT por Intensidade
# ------------------------------------------------------------

library(RPostgreSQL)
library(dplyr)

# Conexão
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, dbname = "cvm_data", host = "localhost", 
                 user = "pesquisador", password = "sua_senha_forte")

# Busca dados brutos
df <- dbGetQuery(con, "SELECT id_documento, indice_restritividade, julgamento_alto, origem_cpc FROM metricas_deonticas")

# Adicionamos uma lógica de pareamento baseada na ordem (assumindo que foram inseridos em pares)
# Ou, preferencialmente, carregamos os metadados se disponíveis.
# Caso não tenha id_par, usamos a divisão da amostra (39 pares)
ifrs <- df %>% filter(origem_cpc == 0) %>% arrange(id_documento)
cpc <- df %>% filter(origem_cpc == 1) %>% arrange(id_documento)

# Unindo os pares
pares_h2 <- data.frame(
  ir_ifrs = ifrs$indice_restritividade,
  ir_cpc = cpc$indice_restritividade,
  julgamento = ifrs$julgamento_alto
) %>%
  mutate(crt = ir_cpc / ir_ifrs)

# Gerando as Estatísticas para a Tabela 4
tabela_4 <- pares_h2 %>%
  group_by(julgamento) %>%
  summarise(
    N = n(),
    Media_CRT = round(mean(crt), 2),
    Mediana_CRT = round(median(crt), 2),
    DP = round(sd(crt), 2),
    Min = round(min(crt), 2),
    Max = round(max(crt), 2)
  )

print(tabela_4)
dbDisconnect(con)