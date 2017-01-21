import numpy as np

x = np.array([1.,2.,3.])
y = np.array([4.,5.,6.])
x = np.array(y)
print(x)
y[0] = 1.
print(x)