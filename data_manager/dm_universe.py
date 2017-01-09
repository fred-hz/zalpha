from pipeline.module import DataPortalModule
from data_manager.dm_base import DataManagerBase
import numpy as np

class DataManagerUniverse(DataManagerBase):
    def caches(self):
        pass

    def dependencies(self):
        self.register_dependency('is_open')

    def provide_data(self):
        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)

        self.is_valid = np.ndarray((di_size, ii_size))

        self.register_data('is_valid', self.is_valid)

    def compute_day(self, di):
        pass

    def initialize(self):
        pass
