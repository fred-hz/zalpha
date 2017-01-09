from pipeline.module import Module
from abc import (
    ABCMeta,
    abstractmethod
)
import numpy as np

class Position(Module):
    __metaclass__ = ABCMeta

    @abstractmethod
    def process_day(self, di):
        """
        Do things to self.alpha if needed
        Return in the form of
        {
            'pnl': xxx,
            'long_capital': xxx,
            'short_capital': xxx,
            'long_num': xxx,
            'short_num': xxx,
            'tvr': xxx
        }
        """
        raise NotImplementedError

class AlphaPosition(Position):
    def initialize(self):
        self.alpha = self.context.alpha
        self.is_long = self.params['is_long']
        self.capital = self.params['capital']
        self.start_di = self.context.start_di
        self.end_di = self.context.end_di

        self.adj_close = self.context.fetch_data('adj_close')
        self.adj_vwap = self.context.fetch_data('adj_vwap')

        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)
        self.history_position = np.ndarray((di_size, ii_size))

    def register_dependencies(self):
        self.register_single_dependency('adj_close')
        self.register_single_dependency('adj_vwap')

    '''------------------------to be implemented-----------------------------'''
    def process_day(self, di):
        """
        Return in the form of
        {
            'pnl': xxx,
            'long_capital': xxx,
            'short_capital': xxx,
            'long_num': xxx,
            'short_num': xxx,
            'tvr': xxx
        }
        """
    '''----------------------------------------------------------------------'''

class AlphaTopPosition(Position):
    def initialize(self):
        self.alpha = self.context.alpha
        self.is_long = self.params['is_long']
        self.capital = self.params['capital']
        self.start_di = self.context.start_di
        self.end_di = self.context.end_di

        self.adj_close = self.context.fetch_data('adj_close')
        self.adj_vwap = self.context.fetch_data('adj_vwap')

        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)
        self.history_position = np.ndarray((di_size, ii_size))

        self.topN = self.params['topN']

    def register_dependencies(self):
        self.register_single_dependency('adj_close')
        self.register_single_dependency('adj_vwap')

    '''------------------------to be implemented-----------------------------'''
    def process_day(self, di):
        """
        Return in the form of
        {
            'pnl': xxx,
            'long_capital': xxx,
            'short_capital': xxx,
            'long_num': xxx,
            'short_num': xxx,
            'tvr': xxx
        }
        """
    '''----------------------------------------------------------------------'''

class IndexPosition(Position):
    def __init__(self, params, context):
        super(IndexPosition, self).__init__(params, context)
        self.index_name = self.params['index_name']

    def initialize(self):
        self.is_long = self.params['is_long']
        self.capital = self.capital['capital']
        self.start_di = self.context.start_di
        self.end_di = self.context.end_di

        self.index_price = self.context.fetch_data(self.index_name)

    def register_dependencies(self):
        self.register_single_dependency(self.index_name)

    '''------------------------to be implemented-----------------------------'''
    def process_day(self, di):
        """
        Return in the form of
        {
            'pnl': xxx,
            'long_capital': xxx,
            'short_capital': xxx,
            'long_num': nan,
            'short_num': nan,
            'tvr': nan
        }
        """
    '''----------------------------------------------------------------------'''

