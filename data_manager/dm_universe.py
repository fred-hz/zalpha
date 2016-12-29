from data_manager.dm_basedata import DataManagerCacheable

class UniverseDataManager(DataManagerCacheable):
    def __init__(self, mid, context, params):
        super(UniverseDataManager, self).__init__(mid=mid, context=context, cache_path=params['cachePath'])

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
