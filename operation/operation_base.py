from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.module import Module
from decorator import assert_exist

class OperationBase(Module):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        # params is supposed to be a list
        self.params = params
        self.context = context

    @abstractmethod
    def initialize(self):
        raise NotImplementedError

    @abstractmethod
    def compute_day(self, di, alpha=None):
        raise NotImplementedError
