from .dm_base import (
    DataManagerBase,
    DataManagerCacheable
)


class DataManagerBaseData(DataManagerBase):

    def initialize(self):
        pass

    def load_day(self, di):
        pass

    def compute_day(self, di):
        pass

    def register_data(self):
        pass

    def register_dependency(self):
        pass
