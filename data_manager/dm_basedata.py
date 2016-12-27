from .dm_base import (
    DataManagerBase,
    DataManagerCacheable
)
import numpy as np


class DataManagerBaseData(DataManagerCacheable):
    def __init__(self, mid, context, params):
        self.data_path = params['dataPath']

        di_size = len(context.di_list)
        ii_size = len(context.ii_list)

        self.open = np.ndarray((di_size, ii_size))
        self.close = np.ndarray((di_size, ii_size))
        super(DataManagerBaseData, self).__init__(mid=mid, context=context)

    def register_caches(self):
        self.register_serialization(self.open)
        self.register_serialization(self.close)

    def initialize(self):
        pass

    def register_data(self):
        self.register_single_data('open', self.open)
        self.register_single_data('close', self.close)

    def compute_day(self, di):
        # To be implemented here
        pass

    def register_dependency(self):
        # Do not need dependencies in DataManagerBaseData
        pass
