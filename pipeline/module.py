from abc import (
    ABCMeta,
    abstractmethod
)

class Module(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def initialize(self):
        raise NotImplementedError

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    @abstractmethod
    def after_day(self, di, alpha):
        raise NotImplementedError
