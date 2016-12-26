from operation.operation_base import OperationBase
import util

class OperationPower(OperationBase):
    def __init__(self, params, context):
        super().__init__(context)
        self.exp = float(params['exp'])
        if not 'dorank' in params:
            self.dorank = True
        else:
            self.dorank = ('True' == params['dorank'])

    def refresh(self, di, alpha):
        # Should return alpha as a list
        if self.dorank:
            util.power(alpha, self.exp)
        else:
            util.powerNoRank(alpha, self.exp)
        return alpha