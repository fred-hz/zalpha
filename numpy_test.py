import numpy as np
from sklearn import linear_model

x = np.array([(1., 1., 1., 1., 1.), (1., -2., 3., 2., -3.), (4., 1., -2., 3., 2.)])
print(x[np.arange(2), 2])