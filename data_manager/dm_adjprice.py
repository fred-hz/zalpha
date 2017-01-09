from pipeline.module import DailyLoopDataPortalModule
import numpy as np
import re


class DataManagerAdjPrice(DailyLoopDataPortalModule):
    def start_day(self, di):
        pass

    def end_day(self, di):
        pass

    def intro_day(self, di):
        pass

    def build(self):
        pass

    def dependencies(self):
        self.register_dependency('open')
        self.register_dependency('high')
        self.register_dependency('low')
        self.register_dependency('close')
        self.register_dependency('vwap')
        self.register_dependency('accumAdjFactor')

    def provide_data(self):
        self.backdays = self.context.fetch_data('backdays')
        self.ii_size = len(self.context.ii_list)

        self.open = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.high = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.low = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.close = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.vwap = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.ret = np.ndarray((self.backdays, self.ii_size), dtype=float)

        self.register_data('adj_open', self.open)
        self.register_data('adj_high', self.high)
        self.register_data('adj_low', self.low)
        self.register_data('adj_close', self.close)
        self.register_data('adj_vwap', self.vwap)
        self.register_data('adj_ret', self.ret)

    def initialize(self):
        self.backdays = self.context.fetch_data('backdays')
        self.rawopen = self.context.fetch_data('open')
        self.rawhigh = self.context.fetch_data('high')
        self.rawlow = self.context.fetch_data('low')
        self.rawclose = self.context.fetch_data('close')
        self.rawvwap = self.context.fetch_data('vwap')
        self.accumAdjFactor = self.context.fetch_data('accumAdjFactor')