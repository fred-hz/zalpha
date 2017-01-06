from .dm_base import (
    DataManagerBase,
    DataManagerCacheable
)
import numpy as np
import re


class DataManagerBaseData(DataManagerCacheable):

    def initialize(self):
        # Fetch data from context and identify values to variables
        self.data_path = self.params['dataPath']
        self.sector_path = self.params['sectorPath']

    def register_data(self):
        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)

        self.CName = []
        for di in range(di_size):
            temp = []
            for ii in range(ii_size):
                temp.append('nan')
            self.CName.append(temp)

        self.isOpen = np.ndarray((di_size, ii_size), dtype=float)
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
        
        self.sector = np.ndarray((di_size, ii_size), dtype=float)
        self.sectorIdx = []
        self.sectorName = []
        self.industry = np.ndarray((di_size, ii_size), dtype=float)
        self.industryIdx = []
        self.industryName = []
        self.subindustry = np.ndarray((di_size, ii_size), dtype=float)
        self.subindustryIdx = []
        self.subindustryName = []

        self.register_single_data('isOpen', self.isOpen)
        self.register_single_data('CName', self.CName)
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
        
        self.register_single_data('sector', self.sector)
        self.register_single_data('sectorIdx', self.sectorIdx)
        self.register_single_data('sectorName', self.sectorName)
        self.register_single_data('industry', self.industry)
        self.register_single_data('industryIdx', self.industryIdx)
        self.register_single_data('industryName', self.industryName)
        self.register_single_data('subindustry', self.subindustry)
        self.register_single_data('subindustryIdx', self.subindustryIdx)
        self.register_single_data('subindustryName', self.subindustryName)


    def register_caches(self):
        self.register_serialization('isOpen')
        self.register_serialization('CName')
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

        self.register_serialization('sector')
        self.register_serialization('sectorIdx')
        self.register_serialization('sectorName')
        self.register_serialization('industry')
        self.register_serialization('industryIdx')
        self.register_serialization('industryName')
        self.register_serialization('subindustry')
        self.register_serialization('subindustryIdx')
        self.register_serialization('subindustryName')

        
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
                ii = self.context.ticker_idx(ticker)
                self.isOpen[di][ii] = float(items[-1])
                if self.isOpen[di][ii] > 0.5:
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
                    self.CName[di][ii] = items[2]
                else:
                    print("warning : there is no company name data of ticker : " + ticker + " at date: " + date)

                if not items[15] == '':
                    self.accumAdjFactor[di][ii] = float(items[15])
                else:
                    print("warning : there is no accumulated adjust factor data of ticker : " + ticker + " at date: " + date)
                    self.accumAdjFactor[di][ii] = np.nan

        try:
            with open(self.sector_path + '\\' + date + '.csv') as fp:
                content = fp.read().splitlines()
        except IOError:
            print("Warning : " + self.sector_path + " : " + date + " sector file is missing")
        else:
            if di > 0:
                self.sector[di] = self.sector[di-1]
                self.industry[di] = self.industry[di-1]
                self.subindustry[di] = self.subindustry[di-1]

            for line in content[1:]:
                items = line.replace('"', '').split(',')
                ticker = re.sub('\D', '', items[1])
                ii = self.context.ticker_idx(ticker)

                sectorSymbol = items[9][:2]
                mark = -1
                for i in range(len(self.sectorIdx)):
                    if self.sectorIdx[i] == sectorSymbol:
                        mark = i
                        break

                if mark == -1:
                    mark = len(self.sectorIdx)
                    self.sectorIdx.append(sectorSymbol)
                    self.sectorName.append(items[13])
                self.sector[di][ii] = mark

                industrySymbol = items[9][:4]
                mark = -1
                for i in range(len(self.industryIdx)):
                    if self.industryIdx[i] == industrySymbol:
                        mark = i
                        break

                if mark == -1:
                    mark = len(self.industryIdx)
                    self.industryIdx.append(industrySymbol)
                    self.industryName.append(items[15])
                self.industry[di][ii] = mark

                subindustrySymbol = items[9]
                mark = -1
                for i in range(len(self.subindustryIdx)):
                    if self.subindustryIdx[i] == subindustrySymbol:
                        mark = i
                        break

                if mark == -1:
                    mark = len(self.subindustryIdx)
                    self.subindustryIdx.append(subindustrySymbol)
                    self.subindustryName.append(items[17])
                self.subindustry[di][ii] = mark

                
    def register_dependency(self):
        # Do not need dependencies in DataManagerBaseData
        pass
