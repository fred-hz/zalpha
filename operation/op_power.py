from operation.operation_base import OperationBase
import util

class OperationPower(OperationBase):

    def initialize(self):
        self.exp = float(self.params['exp'])
        if 'dorank' not in self.params:
            self.do_rank = True
        else:
            self.do_rank = ('True' == self.params['dorank'])

    def after_day(self, di, alpha):
        # Should return alpha as a list
        if self.do_rank:
            util.power(alpha, self.exp)
        else:
            util.powerNoRank(alpha, self.exp)
        return alpha
