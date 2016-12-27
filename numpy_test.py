import numpy as np

x = np.array([np.nan,5.,np.nan])
y = np.array([np.nan, 1.,2.,3.,-2.,-1.,4.,-5.,np.nan,5.,np.nan])
print(np.where(-np.isnan(y)))
print(y[np.where(-np.isnan(y))])