from abc import (
    ABCMeta,
    abstractmethod
)

class OperationBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, params):
        # params is supposed to be a list
        pass

    @abstractmethod
    def refresh(self, di, alpha):
        raise NotImplementedError

