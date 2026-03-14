# ─── R_-_plm.r ───────────────────────────────────────────────────────────────
# Análise de dados de painel — Empresas não-financeiras listadas na B3
# Modelo: Efeitos Fixos Two-Way (empresa + ano) com erros clusterizados
# ─────────────────────────────────────────────────────────────────────────────

library(plm)
library(lmtest)
library(sandwich)
library(RPostgres)
library(DBI)
library(dotenv)

# ─── 1. CONEXÃO COM O BANCO ──────────────────────────────────────────────────
# Carrega o .env — por padrão busca na pasta de trabalho atual
load_dot_env()                          # lê .env da pasta atual
# load_dot_env("d:/VSCode/cvm_data/.env")  # ou caminho explícito

# Variáveis ficam disponíveis via Sys.getenv()
con <- dbConnect(
  RPostgres::Postgres(),
  host     = Sys.getenv("DB_HOST"),
  port     = as.integer(Sys.getenv("DB_PORT")),
  dbname   = Sys.getenv("DB_NAME"),
  user     = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)
# ─── 2. CARREGA DADOS DA VIEW GOLD ───────────────────────────────────────────
# Substitua a query abaixo pela view/tabela que contiver suas variáveis
# dependente e independentes quando as DFPs forem carregadas.
# Por enquanto carrega o painel estrutural de empresas.

painel_raw <- dbGetQuery(con, "
  SELECT
      id_cad_cia_aberta,
      ano,
      ativa_no_ano,
      ano_entrada,
      ano_saida,
      status_negociacao
  FROM cvm_data.vw_painel_empresas
  WHERE ativa_no_ano = TRUE
  ORDER BY id_cad_cia_aberta, ano
")

dbDisconnect(con)
cat("Registros carregados:", nrow(painel_raw), "\n")
cat("Empresas únicas:     ", length(unique(painel_raw$id_cad_cia_aberta)), "\n")
cat("Anos cobertos:       ", min(painel_raw$ano), "a", max(painel_raw$ano), "\n")

# ─── 3. PREPARA O PAINEL ─────────────────────────────────────────────────────
# IMPORTANTE: nunca use "data", "df", "c", "t" como nome de variável em R
painel <- pdata.frame(
  painel_raw,
  index = c("id_cad_cia_aberta", "ano")   # (entidade, tempo)
)

# Verifica estrutura do painel
cat("\nEstrutura do painel:\n")
print(pdim(painel))   # mostra se é balanceado/não balanceado, N e T

# ─── 4. MODELO DE EFEITOS FIXOS TWO-WAY ──────────────────────────────────────
# !! ATENÇÃO !!
# As variáveis y (dependente), x1, x2, x3 (independentes) abaixo são
# EXEMPLOS — substitua pelos campos reais quando as DFPs forem carregadas.
# Exemplo real futuro: y = roa (retorno sobre ativos), x1 = alavancagem,
#                      x2 = tamanho (log_ativo_total), x3 = crescimento_receita

# Descomente e ajuste quando tiver as variáveis:
# modelo_fe <- plm(
#   y ~ x1 + x2 + x3,
#   data   = painel,
#   model  = "within",     # efeitos fixos
#   effect = "twoways"     # empresa + ano (two-way FE)
# )
#
# ─── 5. ERROS PADRÃO CLUSTERIZADOS POR EMPRESA ───────────────────────────────
# coeftest(modelo_fe, vcov = vcovHC(modelo_fe, cluster = "group"))
#
# ─── 6. TESTE DE HAUSMAN (FE vs RE) ──────────────────────────────────────────
# modelo_re <- plm(y ~ x1 + x2 + x3, data = painel,
#                  model = "random", effect = "twoways")
# phtest(modelo_fe, modelo_re)   # H0: RE consistente; rejeitar → usar FE
#
# ─── 7. TESTE F PARA EFEITOS FIXOS ───────────────────────────────────────────
# pFtest(modelo_fe, lm(y ~ x1 + x2 + x3, data = painel_raw))

# ─── 8. ESTATÍSTICAS DESCRITIVAS DO PAINEL ───────────────────────────────────
cat("\nDistribuição de empresas por ano:\n")
print(table(painel_raw$ano))

cat("\nEmpresas por status_negociacao:\n")
print(table(painel_raw$status_negociacao))

cat("\n✓ Script carregado com sucesso.")
cat("\n  Próximo passo: carregar DFPs e adicionar variáveis financeiras ao painel.\n")