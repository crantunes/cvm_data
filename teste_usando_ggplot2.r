
# gerar dados aleatórios
dados <- data.frame(
  x = rnorm(1000)
)

# histograma
ggplot(dados, aes(x = x)) +
  geom_histogram(bins = 30, fill = "steelblue", color = "black") +
  labs(
    title = "Histograma de teste",
    x = "Valores",
    y = "Frequência"
  ) +
  theme_minimal()