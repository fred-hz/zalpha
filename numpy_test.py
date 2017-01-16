import numpy as np

x = np.arange(5, dtype=float)
x[0] = np.nan
print(x)
print(np.nanargmax(x))
