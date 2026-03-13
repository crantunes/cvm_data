import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

x = np.random.normal(0,1,1000)

sns.histplot(x, bins=30)

plt.title("Histograma de teste")
plt.show()