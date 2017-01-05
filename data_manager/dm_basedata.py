from .dm_base import (
    DataManagerBase,
    DataManagerCacheable
)
import numpy as np
import re


class DataManagerBaseData(DataManagerCacheable):

    def initialize(self):
        # Fetch data from context and identify values to variables
        pass

    def register_data(self):
        self.data_path = self.params['dataPath']
        self.sector_path = self.params['sectorPath']

        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)

        self.isOpen = np.ndarray((di_size, ii_size), dtype=bool)
        self.isST = np.ndarray((di_size, ii_size), dtype=bool)
        self.open = np.ndarray((di_size, ii_size), dtype=float)
        self.high = np.ndarray((di_size, ii_size), dtype=float)
        self.low = np.ndarray((di_size, ii_size), dtype=float)
        self.close = np.ndarray((di_size, ii_size), dtype=float)
        self.volume = np.ndarray((di_size, ii_size), dtype=float)
        self.amount = np.ndarray((di_size, ii_size), dtype=float)
        self.turnover = np.ndarray((di_size, ii_size), dtype=float)
        self.cap = np.ndarray((di_size, ii_size), dtype=float)  # 流通市值
        self.vwap = np.ndarray((di_size, ii_size), dtype=float)
        self.accumAdjFactor = np.ndarray((di_size, ii_size), dtype=float)
        self.sharesout = np.ndarray((di_size, ii_size), dtype=float)  # 流通股数

        self.register_single_data('isOpen', self.isOpen)
        self.register_single_data('isST', self.isST)
        self.register_single_data('open', self.open)
        self.register_single_data('high', self.high)
        self.register_single_data('low', self.low)
        self.register_single_data('close', self.close)
        self.register_single_data('volume', self.volume)
        self.register_single_data('amount', self.amount)
        self.register_single_data('turnover', self.turnover)
        self.register_single_data('cap', self.cap)
        self.register_single_data('vwap', self.vwap)
        self.register_single_data('adjfactor', self.accumAdjFactor)
        self.register_single_data('sharesout', self.sharesout)

    def register_caches(self):
        self.register_serialization('isOpen')
        self.register_serialization('isST')
        self.register_serialization('open')
        self.register_serialization('high')
        self.register_serialization('low')
        self.register_serialization('close')
        self.register_serialization('volume')
        self.register_serialization('amount')
        self.register_serialization('turnover')
        self.register_serialization('cap')
        self.register_serialization('vwap')
        self.register_serialization('accumAdjFactor')
        self.register_serialization('sharesout')

    def compute_day(self, di):
        date = self.context.di_list[di]
        print("DataManagerBaseData::computeDay: " + str(di) + " " + date)
        try:
            with open(self.data_path + '\\' + date + '.csv') as fp:
                content = fp.read().splitlines()
        except IOError:
            print("Warning : " + self.data_path + " : " + date + " price file is missing")
        else:
            for line in content[1:]:
                items = line.replace('"','').split(',')
                ticker = re.sub('\D', '', items[1])
                ii = self.context.ii_list.index(ticker)
                self.isOpen[di][ii] = bool(int(items[-1]))
                if self.isOpen[di][ii]:
                    if not items[7] == '':
                        if float(items[7]) < 1e-5:
                            print("warning : there is abnormal value : open of ticker : " + ticker + " at date: " + date)
                        self.open[di][ii] = float(items[7])
                    else:
                        print("warning : there is no open price of ticker : " + ticker + " at date: " + date)
                        self.open[di][ii] = np.nan
                    if not items[8] == '':
                        if float(items[8]) < 1e-5:
                            print("warning : there is abnormal value : high of ticker : " + ticker + " at date: " + date)
                        self.high[di][ii] = float(items[8])
                    else:
                        print("warning : there is no high price of ticker : " + ticker + " at date: " + date)
                        self.high[di][ii] = np.nan
                    if not items[9] == '':
                        if float(items[9]) < 1e-5:
                            print("warning : there is abnormal value : low of ticker : " + ticker + " at date: " + date)
                        self.low[di][ii] = float(items[9])
                    else:
                        print("warning : there is no low price of ticker : " + ticker + " at date: " + date)
                        self.low[di][ii] = np.nan
                    if not items[10] == '':
                        if float(items[10]) < 1e-5:
                            print("warning : there is abnormal value : close of ticker : " + ticker + " at date: " + date)
                        self.close[di][ii] = float(items[10])
                    else:
                        print("warning : there is no close price of ticker : " + ticker + " at date: " + date)
                        self.close[di][ii] = np.nan
                    if not items[11] == '':
                        if float(items[11]) < 1e-5:
                            print("warning : there is abnormal value : volume of ticker : " + ticker + " at date: " + date)
                        self.volume[di][ii] = float(items[11])
                    else:
                        print("warning : there is no volume price of ticker : " + ticker + " at date: " + date)
                        self.volume[di][ii] = np.nan
                    if not items[12] == '':
                        if float(items[12]) < 1e-5:
                            print("warning : there is abnormal value : vwap of ticker : " + ticker + " at date: " + date)
                        self.vwap[di][ii] = float(items[12]) / self.volume[di][ii]
                    else:
                        print("warning : there is no vwap price of ticker : " + ticker + " at date: " + date)
                        self.vwap[di][ii] = np.nan
                    if not items[13] == '':
                        if float(items[13]) < 1e-5:
                            print("warning : there is abnormal value : amount of ticker : " + ticker + " at date: " + date)
                        self.amount[di][ii] = float(items[13])
                    else:
                        print("warning : there is no amount price of ticker : " + ticker + " at date: " + date)
                        self.amount[di][ii] = np.nan
                    if not items[16] == '':
                        if float(items[16]) < 1e-5:
                            print("warning : there is abnormal value : cap of ticker : " + ticker + " at date: " + date)
                        self.cap[di][ii] = float(items[16])
                    else:
                        print("warning : there is no cap price of ticker : " + ticker + " at date: " + date)
                        self.cap[di][ii] = np.nan
                    self.sharesout[di][ii] = self.cap[di][ii] / self.close[di][ii]
                    self.turnover[di][ii] = self.volume[di][ii] / self.sharesout[di][ii]
                else:
                    self.open[di][ii] = np.nan
                    self.high[di][ii] = np.nan
                    self.low[di][ii] = np.nan
                    self.close[di][ii] = np.nan
                    self.volume[di][ii] = np.nan
                    self.vwap[di][ii] = np.nan
                    self.amount[di][ii] = np.nan
                    self.turnover[di][ii] = np.nan
                    self.cap[di][ii] = np.nan
                    self.sharesout[di][ii] = np.nan

                if not items[2] == '':
                    if 'ST' in items[2] or 'st' in items[2]:
                        self.isST[di][ii] = True
                    else:
                        self.isST[di][ii] = False
                else:
                    print("warning : there is no company name data of ticker : " + ticker + " at date: " + date)
                    self.isST[di][ii] = np.nan
                if not items[15] == '':
                    self.accumAdjFactor[di][ii] = float(items[15])
                else:
                    print("warning : there is no accumulated adjust factor data of ticker : " + ticker + " at date: " + date)
                    self.accumAdjFactor[di][ii] = np.nan

    def register_dependency(self):
        # Do not need dependencies in DataManagerBaseData
        pass
