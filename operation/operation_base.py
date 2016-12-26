from abc import (
    ABCMeta,
    abstractmethod
)

class OperationBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, context):
        # params is supposed to be a list
        self.context = context

    @abstractmethod
    def refresh(self, di, alpha):
        raise NotImplementedError

