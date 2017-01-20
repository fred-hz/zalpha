from pipeline.module import DailyLoopDataPortalModule
import numpy as np


class DataManagerAdjPrice(DailyLoopDataPortalModule):
    def initialize(self):
        self.start_date = self.context.start_date
        self.backdays = int(self.context.fetch_constant('backdays'))
        self.start_di = self.context.date_idx(self.start_date)
        self.rawopen = self.context.fetch_data('open')
        self.rawhigh = self.context.fetch_data('high')
        self.rawlow = self.context.fetch_data('low')
        self.rawclose = self.context.fetch_data('close')
        self.rawvwap = self.context.fetch_data('vwap')
        self.accumAdjFactor = self.context.fetch_data('accumAdjFactor')

    def build(self):
        di = self.start_di - 1
        if di < self.backdays - 1:
            mark = di + 1
        else:
            mark = self.backdays
        for i in range(mark):
            adjfactor = self.accumAdjFactor[di - i] / self.accumAdjFactor[di]
            self.open[di - i] = self.open[di - i] * adjfactor
            self.high[di - i] = self.high[di - i] * adjfactor
            self.low[di - i] = self.low[di - i] * adjfactor
            self.close[di - i] = self.close[di - i] * adjfactor
            self.vwap[di - i] = self.vwap[di - i] * adjfactor

    def start_day(self, di):
        pass

    def intro_day(self, di):
        pass

    def end_day(self, di):
        if di < self.backdays - 1:
            mark = di + 1
        else:
            mark = self.backdays

        self.open[di][:] = self.rawopen[di][:]
        self.high[di][:] = self.rawhigh[di][:]
        self.low[di][:] = self.rawlow[di][:]
        self.close[di][:] = self.rawclose[di][:]
        self.vwap[di][:] = self.rawvwap[di][:]

        adjfactor = self.accumAdjFactor[di - 1] / self.accumAdjFactor[di]
        tmp =  abs(1 - adjfactor) > 1e-4
        for i in range(1, mark):
            self.open[di - i][tmp] = self.open[di - i][tmp] * adjfactor[tmp]
            self.high[di - i][tmp] = self.high[di - i][tmp] * adjfactor[tmp]
            self.low[di - i][tmp] = self.low[di - i][tmp] * adjfactor[tmp]
            self.close[di - i][tmp] = self.close[di - i][tmp] * adjfactor[tmp]
            self.vwap[di - i][tmp] = self.vwap[di - i][tmp] * adjfactor[tmp]

    def dependencies(self):
        self.register_dependency('open')
        self.register_dependency('high')
        self.register_dependency('low')
        self.register_dependency('close')
        self.register_dependency('vwap')
        self.register_dependency('accumAdjFactor')

    def provide_data(self):
        self.di_size = len(self.context.di_list)
        self.ii_size = len(self.context.ii_list)

        self.open = np.ndarray((self.di_size, self.ii_size), dtype=float)
        self.high = np.ndarray((self.di_size, self.ii_size), dtype=float)
        self.low = np.ndarray((self.di_size, self.ii_size), dtype=float)
        self.close = np.ndarray((self.di_size, self.ii_size), dtype=float)
        self.vwap = np.ndarray((self.di_size, self.ii_size), dtype=float)

        self.register_data('adj_open', self.open)
        self.register_data('adj_high', self.high)
        self.register_data('adj_low', self.low)
        self.register_data('adj_close', self.close)
        self.register_data('adj_vwap', self.vwap)
