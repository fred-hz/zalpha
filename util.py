import numpy as np


def create_nan_array(n):
    ret = np.empty(n)
    for i in range(0, n):
        ret[i] = np.nan
    return ret


if __name__ == "__main__":
    test = create_nan_array(3)
    print(test)