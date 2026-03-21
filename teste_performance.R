# -------------------------------------------------------------------------
# SCRIPT DE TESTE: AMBIENTE VS CODE + R
# Objetivo: Validar instalação de pacotes, processamento e gráficos
# -------------------------------------------------------------------------

# 1. Carregamento de Bibliotecas
if (!require("tidyverse")) install.packages("tidyverse")
library(tidyverse)

# 2. Criação de Dados Sintéticos (Simulação de Retornos Financeiros)
set.seed(42)
df_teste <- data.frame(
  periodo = seq(1, 100),
  retorno = rnorm(100, mean = 0.05, sd = 0.02),
  setor = sample(c("Tech", "Energy", "Retail"), 100, replace = TRUE)
)

# 3. Manipulação de Dados (Dplyr)
resumo_setor <- df_teste %>%
  group_by(setor) %>%
  summarise(
    media_retorno = mean(retorno),
    volatilidade = sd(retorno)
  )

print("--- Resumo Estatístico por Setor ---")
print(resumo_setor)

# 4. Visualização de Dados (Ggplot2)
# Este comando abrirá uma janela de plotagem no VS Code
ggplot(df_teste, aes(x = periodo, y = retorno, color = setor)) +
  geom_line(size = 1) +
  geom_smooth(method = "lm", se = FALSE, linetype = "dashed") +
  theme_minimal() +
  labs(
    title = "Teste de Visualização: Retornos Simulados",
    subtitle = "Integração R + VS Code",
    x = "Período T",
    y = "Taxa de Retorno"
  )