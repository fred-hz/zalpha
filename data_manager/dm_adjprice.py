from .dm_base import (
    DataManagerBase
)
import numpy as np
import re


class DataManagerAdjPrice(DataManagerBase):
    def __init__(self, mid, context, params):
        print("DataManagerAdjPrice initialize!")
        self.mid = mid
        self.context = context
        self.params = params

        self.backdays = self.context.fetch_data('backdays')

        self.ii_size = len(self.context.ii_list)

        self.rawopen = self.context.fetch_data('open')
        self.rawhigh = self.context.fetch_data('high')
        self.rawlow = self.context.fetch_data('low')
        self.rawclose = self.context.fetch_data('close')
        self.rawvwap = self.context.fetch_data('vwap')
        self.accumAdjFactor = self.context.fetch_data('accumAdjFactor')

        self.open = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.high = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.low = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.close = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.vwap = np.ndarray((self.backdays, self.ii_size), dtype=float)
        self.ret = np.ndarray((self.backdays, self.ii_size), dtype=float)

        super(DataManagerAdjPrice, self).__init__(mid=mid, context=context)

    def compute_day(self, di):
        for ii in range(self.ii_size):
            self.open[di][ii] = self.rawopen[di][ii]
            self.high[di][ii] = self.rawhigh[di][ii]
            self.low[di][ii] = self.rawlow[di][ii]
            self.close[di][ii] = self.rawclose[di][ii]
            self.vwap[di][ii] = self.rawvwap[di][ii]

