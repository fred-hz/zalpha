from operation.operation_base import OperationBase
import numpy as np

class OperationNeutral(OperationBase):
    def __init__(self, params, context):
        super().__init__(params, context)
        self.group = context.fetch_data(params['group'])

    def after_day(self, di, alpha):
        # Should return alpha as a list
        # neut_data = {}
        # map(lambda a: neut_data.setdefault(a, (np.where(self.group[di] == a))[0]), set(self.group[di]))
        neut_data = {a: np.where(a == self.group[di])[0] for a in set(self.group[di])}

        means = {a: np.nanmean(alpha[neut_data[a]]) for a in neut_data}
        for key in neut_data:
            alpha[neut_data[key]] -= means[key]

        return alpha