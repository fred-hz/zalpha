from abc import (
    ABCMeta,
    abstractmethod
)
from context import (
    dm,
    date2di,
    di2date,
    ticker2ii,
    ii2ticker
)
import numpy as np


class AlphaBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, mid, para_list=None):
        self.mid = mid
        self.para_list = para_list
        self.alpha = np.array(len(ticker2ii))###

    @abstractmethod
    def initialize(self):
        """
        Set vars of alpha from dm
        :return:
        """
        raise NotImplementedError

    @abstractmethod
    def generate(self, di):
        """
        Staff to be done on date di
        :param di:
        :return:
        """
        raise NotImplementedError

    def compute(self):
        for di in di2date.keys():
            self.generate(di)

