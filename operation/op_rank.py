from operation.operation_base import OperationBase
import util

class OperationRank(OperationBase):
    def __init__(self, context):
        super().__init__(context)

    def refresh(self, di, alpha):
        # Should return alpha as a list
        util.rank(alpha)
        return alpha