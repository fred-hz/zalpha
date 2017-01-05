from operation.operation_base import OperationBase
import util

class OperationTruncate(OperationBase):

    def initialize(self):
        self.maxPercent = float(self.params['maxPercent'])
        if 'maxIter' not in self.params:
            self.maxIter = 1
        else:
            self.maxIter = int(self.params['maxIter'])

    def after_day(self, di, alpha):
        # Should return alpha as a list
        util.truncate(alpha, self.maxPercent, self.maxIter)
        return alpha
