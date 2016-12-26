from operation.operation_base import OperationBase
import util

class OperationTruncate(OperationBase):
    def __init__(self, params, context):
        super().__init__(context)
        self.maxPercent = float(params['maxPercent'])
        if 'maxIter' not in params:
            self.maxIter = 1
        else:
            self.maxIter = int(params['maxIter'])

    def refresh(self, di, alpha):
        # Should return alpha as a list
        util.truncate(alpha, self.maxPercent, self.maxIter)
        return alpha