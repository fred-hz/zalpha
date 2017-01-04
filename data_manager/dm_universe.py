from data_manager.dm_basedata import DataManagerCacheable

class DataManagerUniverse(DataManagerCacheable):
    def __init__(self, context, params):
        super(DataManagerUniverse, self).__init__(context=context, params=params)

    def register_caches(self):
        pass

    def initialize(self):
        pass

    def register_data_names(self):
        pass

    def compute_day(self, di):
        pass

    def register_dependency(self):
        pass
