from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.serialization import Serializable
from pipeline.module import (
    DataPortalModule
)

class DataManagerBase(DataPortalModule):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(DataManagerBase, self).__init__(params, context)
        print("DMBase Initialized")
        self.register_dependency('di_list')
        self.register_dependency('ii_list')
