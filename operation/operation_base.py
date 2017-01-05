from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.module import Module
from decorator import assert_exist

class OperationBase(Module):
    __metaclass__ = ABCMeta

    @abstractmethod
    def initialize(self):
        raise NotImplementedError

    def compute_day(self, di):
        # Operations don't need to compute_day()
        pass

    @abstractmethod
    def after_day(self, di, alpha):
        raise NotImplementedError
