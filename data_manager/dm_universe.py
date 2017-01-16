from pipeline.module import DataPortalModule
from data_manager.dm_base import DataManagerBase
import numpy as np


class DataManagerUniverse(DataManagerBase):
    def initialize(self):
        self.ticker_list = self.context.ii_list
        self.date_list = self.context.di_list
        self.isOpen = self.context.fetch_data('isOpen')
        self.isST = self.context.fetch_data('isST')
        self.cap = self.context.fetch_data('cap')
        self.close = self.context.fetch_data('close')
        self.volume = self.context.fetch_data('volume')
        self.vwap = self.context.fetch_data('vwap')
        self.size = int(self.params['universe'])

    def provide_data(self):
        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)

        self.is_valid = np.ndarray((di_size, ii_size), dtype=float)
        self.register_data(self.params['id'], self.is_valid)

    def compute_day(self, di):
            print('Begin calculate universe: ' + self.context.di_list[di])
            liquidity = {}
            di -= 1  # 用之前的值去决定今天的universe
            for ii in range(len(self.ticker_list)):
                if self.isST[di][ii] > 0.5:  # 去掉ST股票
                    continue

                if self.cap[di][ii] < 1e-5:  # min cap > 0
                    continue

                if self.close[di][ii] <= 1 or self.close[di][ii] >= 1000:  # min price > 1 and max price < 1000
                    continue

                if self.volume[di][ii] < 1e-5:  # min volume > 0
                    continue

                deter = True
                for i in range(40):  # IPO 40天后开始交易
                    if np.isnan(self.isOpen[di - i][ii]):
                        deter = False
                if not deter:
                    continue

                for i in range(5):  # di前连续5天为交易日
                    if self.isOpen[di - i][ii] == 0:
                        # print (isOpen[di-i][ii])
                        deter = False
                if not deter:
                    continue

                total = 0
                num = 0
                for i in range(63):  # 取63天中，平均金额最高的n只股票
                    if self.isOpen[di - i][ii] == 1 and not np.isnan(self.vwap[di - i][ii]) and not np.isnan(
                            self.volume[di - i][ii]):
                        # print(isOpen[di-i][ii])
                        num += 1
                        total += self.vwap[di - i][ii] * self.volume[di - i][ii]
                if num < 1e-5 or total < 1e-5:
                    print("warning : there is error in computing universe of ticker : " + self.ticker_list[ii] + " at date: " + self.date_list[di])
                    continue
                liquidity[ii] = total / num
                # print(liquidity[ii])

            liquidity_ = sorted(list(liquidity.items()), key=lambda d: d[1], reverse=True)
            if len(liquidity) <= self.size:
                for key in liquidity:
                    self.is_valid[di][key] = 1
            else:
                for i in range(self.size):
                    self.is_valid[di][liquidity_[i][0]] = 1

    def dependencies(self):
        self.register_dependency('isOpen')
        self.register_dependency('isST')
        self.register_dependency('cap')
        self.register_dependency('close')
        self.register_dependency('volume')
        self.register_dependency('vwap')

    def caches(self):
        self.register_cache('is_valid')

    def fetch_single_data(self, data_name):
        if data_name == self.params['id']:
            return super(DataManagerUniverse, self).fetch_single_data('is_valid')
        else:
            return super(DataManagerUniverse, self).fetch_single_data(data_name)

    def freshes(self):
        self.register_fresh('is_valid')