from abc import (
    ABCMeta,
    abstractmethod
)
import numpy as np
from pipeline.module import DailyLoopModule

class AlphaBase(DailyLoopModule):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(AlphaBase, self).__init__(params, context)

    def initialize(self):
        self.alpha = self.context.alpha

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError

    def start_day(self, di):
        self.alpha.fill(np.nan)
        self.compute_day(di)

    def end_day(self, di):
        pass

    def intro_day(self, di):
        pass
