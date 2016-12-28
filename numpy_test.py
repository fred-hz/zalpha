import numpy as np

x = np.array([np.nan,5.,np.nan,6.])
y = np.array([np.nan, 1.,2.,3.,-2.,-1.,4.,-5.,np.nan,5.,np.nan])
print(np.sum(x>1))
print(np.sum(y[np.where(-np.isnan(y))[0]]))
