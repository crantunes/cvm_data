# -------------------------------------------------------------------------
# VISUALIZAÇÃO ACADÊMICA DE ALTA DENSIDADE
# -------------------------------------------------------------------------

# 1. Garantir que a biblioteca está carregada
library(tidyverse)

# 2. Gerar o Gráfico de Dispersão com Linha de Tendência
grafico_retornos <- ggplot(df_teste, aes(x = periodo, y = retorno, color = setor)) +
  # Adiciona pontos com transparência (evita overplotting em bases grandes)
  geom_point(alpha = 0.6, size = 2) +
  # Adiciona linha de tendência suave (LOESS) para análise visual de volatilidade
  geom_smooth(method = "loess", se = TRUE, alpha = 0.1) +
  # Aplica um tema limpo, padrão de journals americanos
  theme_minimal() +
  # Customização de rótulos (Labels)
  labs(
    title = "Análise Exploratória: Retornos Simulados por Setor",
    subtitle = "Métrica de validação para o ambiente R 4.5.2 + Radian",
    x = "Período (T)",
    y = "Taxa de Retorno (μ)",
    color = "Setor Analisado"
  ) +
  # Ajustes estéticos finais
  theme(legend.position = "bottom", plot.title = element_text(face = "bold"))

# 3. Exibir o gráfico no VS Code
print(grafico_retornos)
