from abc import (
    ABCMeta,
    abstractmethod
)
import numpy as np
from pipeline.module import Module

class AlphaBase(Module):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        super(AlphaBase, self).__init__(params, context)
        self.alpha = []

    def get_alpha(self):
        return self.alpha

    @abstractmethod
    def compute_day(self, di):
        raise NotImplementedError
