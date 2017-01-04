from abc import (
    ABCMeta,
    abstractmethod
)
import numpy as np


class AlphaBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, params, context):
        self.params = params
        #self.alpha = np.array(len(ticker2ii))###

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
        # for di in di2date.keys():
        #     self.generate(di)
        pass
