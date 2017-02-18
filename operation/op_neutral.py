from operation.operation_base import OperationBase
import numpy as np
from cutils.neut import neut_func


class OperationNeutral(OperationBase):
    def initialize(self):
        self.group = self.context.fetch_data(self.params['group'])

    def compute_day(self, di, alpha):
        # Should return alpha as a list
        # neut_data = {}
        # map(lambda a: neut_data.setdefault(a, (np.where(self.group[di] == a))[0]), set(self.group[di]))

#neut_data = {a: np.where(a == self.group[di])[0] for a in set(self.group[di])} 
#        means = {a: np.nanmean(alpha[neut_data[a]]) for a in neut_data}
#        for key in neut_data:
#            alpha[neut_data[key]] -= means[key]
        neut_func(alpha, self.group[di-1])
        return alpha

    def dependencies(self):
        self.register_dependency('industry')
        self.register_dependency('subindustry')
        self.register_dependency('sector')
