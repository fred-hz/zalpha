from operation.operation_base import OperationBase
import numpy as np

class OperationDecay(OperationBase):
    def __init__(self, params, context):
        super().__init__(params, context)
        self.days = float(params['days'])
        if 'dense' not in params:
            self.dense = True
        else:
            self.dense = ('True' == params['dense'])
        self.valid = self.context.fetch_data('is_valid') # need to revise
        Instruments_size = len(self.context.ii_list)
        self.hist = np.zeros((self.days, Instruments_size))
        self.hist.flat = np.nan
        self.num = np.zeros(Instruments_size, dtype=np.int)
        self.sum = np.zeros(Instruments_size)
        self.sum.flat = np.nan
        self.diff = np.zeros(Instruments_size)
        self.diff.flat = np.nan

    def after_day(self, di, alpha):
        # Should return alpha as a list
        self.sum -= self.diff
        tmp = np.where(self.num == self.days)
        self.diff[tmp] -= self.hist[self.days - 1][tmp] / float(self.days)
        self.num[tmp] -= 1
        self.hist = np.vstack((alpha, self.hist[:-1]))

        tmp = -np.isnan(self.hist[0])
        tmp_zero = np.where(self.num == 0 & -np.isnan(self.hist[0]))
        self.sum[tmp_zero] = 0
        self.diff[tmp_zero] = 0
        self.num[tmp] += 1
        self.sum[tmp] += self.hist[0][tmp]
        self.diff[tmp] += self.hist[0][tmp] / float(self.days)

        tmp_nan = np.isnan(self.hist[0])
        if not self.dense:
            tmp = np.where(self.num > 0 & self.valid[di] == 1 & np.isnan(self.hist[0]))
            self.num[tmp] += 1
            self.hist[0][tmp] = 0
        else:
            self.num[tmp_nan] = 0
            self.sum[tmp_nan] = np.nan
            self.diff[tmp_nan] = np.nan

        alpha = np.array(self.sum)  # copy array

        if self.dense:
            denom = (2 * self.days - self.num) * (self.num + 1) / 2 / self.days
            tmp = (denom > 0)
            alpha[tmp] = alpha[tmp] / denom[tmp]
        return alpha