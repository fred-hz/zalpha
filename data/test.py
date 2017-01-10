import numpy as np

class zzarray(np.ndarray):
    def __init__(self, shape_x, shape_y, real_x):
        self.data = super(zzarray,self).__init__((real_x, shape_y))
        self.shape_x = shape_x
        self.real_x = real_x

    def __getitem__(self, index):
        return self.data[index % self.real_x]

    def __setitem__(self, key, value):
        self.data[key%self.real_x] = value

    def get_list(self):
        return self.data

    def __str__(self):
        return str(self.data)

test = zzarray(5, 5, 3)
print(test)