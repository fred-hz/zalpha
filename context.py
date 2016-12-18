from decorator import (
    assert_class
)
from data_manager.dm_base import (
    DataManagerBase
)

# date-index mapping and ticker-index mapping
di2date = {}    # {di: date} while di is int and date is string
date2di = {}
ii2ticker = {}  # {ii : ticker} while ii is int and ticker is string
ticker2ii = {}

# Register data in data_container.
# The format is {'name': numpy.ndarray}
data_container = {}


class DataPortal(object):
    def __init__(self):
        # Save data modules. In the format of {mid: module_obj}
        self.data_module = {}

        # Store all the data. In the format of {data_name: {'data': data, 'mid': mid}}
        self.data = {}

        # Store all the data dependencies. In the format of {mid: [data_name]}
        self.dependencies = {}

    def add_data_module(self, mid, module_obj):
        assert isinstance(module_obj, DataManagerBase)
        self.data_module[mid] = module_obj

    def register_data(self, mid, data_name, data):
        self.data[data_name] = {'data': data, 'mid': mid}

    def register_dependency(self, mid, data_name):
        if mid not in self.dependencies.keys():
            self.dependencies[mid] = []
        self.dependencies[mid].append(data_name)

dm = DataPortal()
