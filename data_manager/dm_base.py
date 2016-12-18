from abc import (
    ABCMeta,
    abstractmethod
)
import os
from context import (
    dm,
    di2date,
    date2di,
    ii2ticker,
    ticker2ii
)
from pipeline.serialization import Serializable
import pickle


class DataManagerBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, mid, data_path):
        """

        :param mid: Module id.
        :param data_path: DM could load data from file and use it to calculate.
        :param args:
        :param kwargs:
        """
        self.mid = mid
        self.data_path = data_path

        # Store data. In the format of {data_name: data}
        self.data = {}

        # Store dependency. In the format of [data_name]
        self.dependency = []

        self.initialize()
        self.register_dependency()
        self.register_data()

    @abstractmethod
    def initialize(self):
        """
        Define all the vars used. Mainly need to claim their size based on numpy.ndarray
        :return:
        """
        raise NotImplementedError

    def _compute(self):
        """
        Logic loop of self.compute()
        :return:
        """
        """
        for date in [di]:
            self.compute_day(di)
        """
        for di in di2date.keys():
            # If self.data_path exists, we need to load everyday data from file
            if not (self.data_path is None or self.data_path == ''):
                self.load_day(di)
            self.compute_day(di)

    def compute(self):
        """
        The function is a wrapper of self.compute_day(self, di).
        If the object is a DataManageCacheable, the compute will depend on the existance of cache.
        If the object is not, the compute will just call compute_day(self, di) in a loop.
        :return:
        """
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
    def load_day(self, di):
        """
        Load data from file every day
        :param di: Index of date
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
        dm.register_dependency(self.mid, data_name)

    @abstractmethod
    def register_data(self):
        """
        Register data into globals
        :return:
        """
        raise NotImplementedError

    # Call self.register_single_data in self.register_data
    def register_single_data(self, data_name, data):
        dm.register_data(self.mid, data_name, data)


class DataManagerCacheable(DataManagerBase, Serializable):
    """
    Interface to be impelemented by data manager subclass.
    The interface is used when data are derived from raw data and stored in cache file.
    When cache file exists, data manager will directly read from it and not compute.
    Could cache multi matrix at once. Data in self.data will be cached.
    """
    __metaclass__ = ABCMeta

    def __init__(self, mid, data_path, cache_path):
        super(DataManagerCacheable, self).__init__(mid, data_path)
        self.cache_path = cache_path

    def compute(self):
        if not self.cache_exist():
            self._compute()
            # and need to dump matrixes
        else:
            self.load_cache()

    @abstractmethod
    def register_data(self):
        raise NotImplementedError

    @abstractmethod
    def register_dependencies(self):
        raise NotImplementedError

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    def cache_exist(self):
        file_list = os.listdir(self.cache_path)
        return file_list is not None and len(file_list) != 0

    def dump_cache(self):
        for data_name, data in self.data.items():
            self._dump(data_name, data)

    def load_cache(self):
        for data_name in self.data.keys():
            self._load(data_name)

    def _dump(self, data_name, data):
        path = os.path.join(self.cache_path, data_name)
        pickle.dump(data, open(path, 'wb'))

    def _load(self, data_name):
        setattr(self, data_name, pickle.load(os.path.join(self.cache_path, data_name)))

