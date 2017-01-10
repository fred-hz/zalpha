from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.serialization import Serializable

class Module(object):
    # All the modules have dependencies on other modules
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        self.params = params
        self.context = context

        self.dependency = []
        self.dependencies()

    @abstractmethod
    def initialize(self):
        # Fetch data from context and identify value to variables
        raise NotImplementedError

    @abstractmethod
    def dependencies(self):
        # Register data needed
        raise NotImplementedError

    # Call self.register_dependency in self.dependencies
    def register_dependency(self, data_name):
        self.dependency.append(data_name)

class DailyLoopModule(Module):
    __metaclass__ = ABCMeta

    @abstractmethod
    def start_day(self, di):
        # Execute before trading time. Not know any data[di]. Can only use data before di
        raise NotImplementedError

    @abstractmethod
    def intro_day(self, di):
        raise NotImplementedError

    @abstractmethod
    def end_day(self, di):
        # Execute after trading time. Know all data[di]
        raise NotImplementedError

class DataProvider(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        # self.data only records data names.
        # To change the real value of data, we need to change self.xxx
        self.data = {}
        self.provide_data()

    @abstractmethod
    def provide_data(self):
        # Only register data names in self.data with not fulfill object.
        # The values would later be filled.
        raise NotImplementedError

    def register_data(self, data_name, data):
        self.data[data_name] = data

class DailyLoopDataPortalModule(DailyLoopModule, DataProvider):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(DailyLoopDataPortalModule, self).__init__(params, context)

    @abstractmethod
    def build(self):
        # Just a preparing logic before daily loop begins
        raise NotImplementedError

class DataPortalModule(Module, DataProvider, Serializable):
    # Modules that runs before back-test daily loop.
    # Can be regarded as data preparing modules.
    # Provide data as well as depend on other data.
    # Provide cacheable data
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(DataPortalModule, self).__init__(params=params,
                                               context=context,
                                               cache_path=params['cachePath'])

    def build(self):
        start_di = self.context.start_di
        end_di = self.context.end_di
        for di in range(start_di, end_di+1):
            self.compute_day(di)
        self.dump(self.cache_path)
        for name in self.data_loaded.keys():
            self.data_loaded[name] = True

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    def cache_day(self, di):
        """
        Cache data for a new day di based on already cached data
        :param di:
        :return:
        """
        if self.cache_exist(self.cache_path):
            self.load_all_data(self.cache_path)
        else:
            self.build()
        self.compute_day(di)
        self.dump(self.cache_path)

    def fetch_single_data(self, data_name):
        if not self.cache_exist(self.cache_path):
            self.build()
        if self.data_loaded[data_name] is False:
            self.load_single_data(data_name)
            return getattr(self, data_name)



# class DataPortalModuleCacheable(DataPortalModule, Serializable):
#     __metaclass__ = ABCMeta
#
#     def __init__(self, params, context):
#         super(DataPortalModuleCacheable, self).__init__(params=params,
#                                                         context=context,
#                                                         cache_path=params['cachePath'])
#         self.register_caches()
#
#     @abstractmethod
#     def register_caches(self):
#         raise NotImplementedError
#
#     def fetch_data(self, data_names):
#         if self.data_loaded is False:
#             if not self.cache_exist(self.cache_path):
#                 self._compute()
#                 self.dump(self.cache_path)
#             else:
#                 for name in data_names:
#                     self.load_single_data(self.cache_path, name)
#             self.data_loaded = True
#         return {name: self.data[name] for name in data_names}
