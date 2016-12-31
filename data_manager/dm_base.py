from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.serialization import Serializable


class DataManagerBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, mid, context):
        # Always run DataManagerBase.__init__() at the end of the __init__() function of sub class
        self.mid = mid
        self.context = context

        # Store data. In the format of {data_name: data}
        self.data = {}

        # Store dependency. In the format of [data_name]
        self.dependency = []

        self.start_date = context.start_date
        self.end_date = context.end_date

        self.initialize()
        self.register_dependency()
        self.register_data_names()

    @abstractmethod
    def initialize(self):
        """
        Define all the vars used. Mainly need to claim their size based on numpy.ndarray
        :return:
        """
        raise NotImplementedError

    def _compute(self):
        start_di = self.context.date_idx(self.start_date)
        end_di = self.context.date_idx(self.end_date)
        for di in range(start_di, end_di + 1):
            self.compute_day(di)

    def compute(self):
        self._compute()

    @abstractmethod
    def compute_day(self, di):
        """
        Compute data everyday
        :param di: Index of date.
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def register_dependency(self):
        """
        Register denpendencies so that data can be loaded and computed in order
        :return:
        """
        raise NotImplementedError

    # Call self.register_single_dependency in self.register_dependency
    def register_single_dependency(self, data_name):
        self.dependency.append(data_name)

    @abstractmethod
    def register_data_names(self):
        """
        Register data into globals
        :return:
        """
        raise NotImplementedError

    # Call self.register_single_data in self.register_data
    # Just register a position in self.data. Do not need value now.
    def register_single_data(self, data_name, data):
        self.data[data_name] = data

    def get_dependencies(self):
        return self.dependency

    def get_data_names(self):
        return self.data.keys()

    def get_data(self, name):
        return self.data[name]

class DataManagerCacheable(DataManagerBase, Serializable):

    """
    Interface to be impelemented by data manager subclass.
    The interface is used when data are derived from raw data and stored in cache file.
    When cache file exists, data manager will directly read from it and not compute.
    Could cache multi matrix at once. Data in self.data will be cached.
    """
    __metaclass__ = ABCMeta

    def __init__(self, mid, context, cache_path):
        # Always run DataManageCacheable.__init__() at the end of __init__() function of sub class
        super(DataManagerCacheable, self).__init__(mid=mid, context=context, cache_path=cache_path)
        self.register_caches()

    def compute(self):
        if not self.cache_exist(self.cache_path):
            self._compute()
            self.dump(self.cache_path)
        # else:
        #     self.load(self.cache_path)

    @abstractmethod
    def register_caches(self):
        raise NotImplementedError

    @abstractmethod
    def register_data_names(self):
        raise NotImplementedError

    @abstractmethod
    def register_dependency(self):
        raise NotImplementedError

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    def get_data(self, name):
        if self.data[name] is not None:
            return self.data[name]
        elif self.cache_exist(self.cache_path):
            self.data[name] = self.load_single_data(cache_path=self.cache_path, name=name)
            return self.data[name]
        else:
            raise Exception('data {name} can not be found in calculation or cache'.format(
                name=name
            ))

