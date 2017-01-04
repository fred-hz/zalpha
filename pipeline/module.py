from abc import (
    ABCMeta,
    abstractmethod
)

class Module(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def initialize(self):
        raise Exception('Not implemented')

    @abstractmethod
    def compute_day(self, di):
        raise Exception('Not implemented')