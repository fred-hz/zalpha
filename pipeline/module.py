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
        self.register_dependencies()

    @abstractmethod
    def initialize(self):
        # Fetch data from context and identify value to variables
        raise NotImplementedError

    @abstractmethod
    def register_dependencies(self):
        # Register data needed
        raise NotImplementedError

    # Call self.register_single_dependency in self.register_dependency
    def register_single_dependency(self, data_name):
        self.dependency.append(data_name)

    def fetch_dependencies(self):
        return self.dependency

class DataPortalModule(Module):
    # Modules that runs before back-test daily loop.
    # Can be regarded as data preparing modules.
    # Provide data as well as depend on other data.
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(DataPortalModule, self).__init__(params, context)
        self.data = {}
        self.register_data()

        self.data_loaded = False

    @abstractmethod
    def register_data(self):
        # Only register data names in self.data with not fulfill object.
        # The values would later be filled.
        raise NotImplementedError

    def register_single_data(self, data_name, data):
        self.data[data_name] = data

    def fetch_data(self, data_names):
        """
        Return data with names in data_names. Maybe need to compute or load from cache
        :param data_names: A list containing all the names needed
        :return:
        """
        if self.data_loaded is False:
            self._compute()
            self.data_loaded = True
        return {name: self.data[name] for name in data_names}

    def _compute(self):
        start_di = self.context.date_idx(self.context.start_date)
        end_di = self.context.date_idx(self.context.end_date)
        for di in range(start_di, end_di + 1):
            self.compute_day(di)

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

class DataPortalModuleCacheable(DataPortalModule, Serializable):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(DataPortalModuleCacheable, self).__init__(params=params, context=context, cache_path=params['cachePath'])
        self.register_caches()

    @abstractmethod
    def register_caches(self):
        raise NotImplementedError

    def fetch_data(self, data_names):
        if self.data_loaded is False:
            if not self.cache_exist(self.cache_path):
                self._compute()
                self.dump(self.cache_path)
            else:
                for name in data_names:
                    self.load_single_data(self.cache_path, name)
            self.data_loaded = True
        return {name: self.data[name] for name in data_names}