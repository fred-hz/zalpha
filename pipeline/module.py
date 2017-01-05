from abc import (
    ABCMeta,
    abstractmethod
)

class Module(object):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        self.params = params
        self.context = context

    @abstractmethod
    def initialize(self):
        # Fetch data from context and identify value to variables
        raise NotImplementedError

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    @abstractmethod
    def after_day(self, di, alpha):
        raise NotImplementedError
