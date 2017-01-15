from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.serialization import Serializable
import numpy as np

class Module(object):
    # All the modules have dependencies on other modules
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        self.params = params
        self.context = context

        self.dependency = []
        self.dependencies()
        print("Module Initialized")

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

    def __init__(self, params, context):
        super(DailyLoopModule, self).__init__(params, context)
        print("DailyLoopModule Initialized.")

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
        print("DataProvider Initialized.")

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
        print("DailyLoopDataPortalModule Initialized.")

    @abstractmethod
    def build(self):
        # Just a preparing logic before daily loop begins
        raise NotImplementedError

class DataPortalModule(DataProvider, Module, Serializable):
    # Modules that runs before back-test daily loop.
    # Can be regarded as data preparing modules.
    # Provide data as well as depend on other data.
    # Provide cacheable data
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        if params is None:
            cache_path = None
        else:
            cache_path = params['cachePath']
        DataProvider.__init__(self)
        Module.__init__(self, params, context)
        Serializable.__init__(self, cache_path)
        self.refresh_list = []

        print("DataPortalModule Initialized.")

    def build(self):
        start_date = self.params['startDate']
        end_date = self.params['endDate']
        start_di = self.context.date_idx(start_date)
        end_di = self.context.date_idx(end_date)
        for di in range(start_di, end_di+1):
            self.compute_day(di)
        self.dump(self.cache_path)
        for name in self.data_loaded.keys():
            self.data_loaded[name] = True

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    @abstractmethod
    def freshes(self):
        raise NotImplementedError

    def register_fresh(self, data_name):
        self.refresh_list.append(data_name)

    def refresh_day(self, cache_path):
        if len(self.refresh_list) == 0:
            return
        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)
        data = getattr(self, self.refresh_list[0])
        old_di_size, old_ii_size = data.shape
        if old_di_size == di_size:
            return
        for data_name in self.refresh_list:
            data = getattr(self, data_name)
            if ii_size > old_ii_size:
                temp = np.ndarray((di_size, ii_size - old_ii_size), dtype=float)
                temp.flat = np.nan
                data = np.hstack((data, temp))

            temp = np.ndarray((di_size - old_di_size, ii_size), dtype=float)
            temp.flat = np.nan
            setattr(self, data_name, np.vstack((data, temp)))
        for di in range(old_di_size, di_size):
            self.compute_day(di)
        self.dump(cache_path)

    def cache_day(self, di):
        """
        Cache data for a new day di based on already cached data
        :param di:
        :return:
        """
        if self.cache_exist(self.cache_path):
            self.load_all_data(self.cache_path)
            self.refresh_day(self.cache_path)
        else:
            self.build()
            self.dump(self.cache_path)

    def fetch_single_data(self, data_name):
        if not self.cache_exist(self.cache_path):
            self.build()
        if self.data_loaded[data_name] is False:
            self.load_single_data(self.cache_path, data_name)
            return getattr(self, data_name)
