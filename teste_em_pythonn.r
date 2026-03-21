import numpy as np
import matplotlib.pyplot as plt

# gerar dados
x = np.random.normal(0, 1, 1000)

# histograma
plt.hist(x, bins=30)

plt.title("Histograma de teste")
plt.xlabel("Valores")
plt.ylabel("Frequência")

plt.show()