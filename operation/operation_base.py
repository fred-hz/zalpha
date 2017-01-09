from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.module import Module
from decorator import assert_exist

class OperationBase(Module):
    __metaclass__ = ABCMeta

    @abstractmethod
    def compute_day(self, di, alpha):
        raise NotImplementedError

    def register_dependencies(self):
        pass
