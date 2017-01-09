from abc import (
    ABCMeta,
    abstractmethod
)
from pipeline.module import DailyLoopModule
from decorator import assert_exist

class OperationBase(DailyLoopModule):
    __metaclass__ = ABCMeta

    @abstractmethod
    def compute_day(self, di, alpha):
        raise NotImplementedError

    def start_day(self, di):
        pass

    def intro_day(self, di):
        pass

    def end_day(self, di):
        self.compute_day(di, self.context.alpha)

    def dependencies(self):
        pass
