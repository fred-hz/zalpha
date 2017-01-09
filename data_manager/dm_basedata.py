from data_manager.dm_base import DataManagerBase
import numpy as np
import re


class DataManagerBaseData(DataManagerBase):

    def initialize(self):
        # Fetch data from context and identify values to variables
        self.data_path = self.params['dataPath']
        self.sector_path = self.params['sectorPath']

    def provide_data(self):
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

        self.register_data('isOpen', self.isOpen)
        self.register_data('CName', self.CName)
        self.register_data('open', self.open)
        self.register_data('high', self.high)
        self.register_data('low', self.low)
        self.register_data('close', self.close)
        self.register_data('volume', self.volume)
        self.register_data('amount', self.amount)
        self.register_data('turnover', self.turnover)
        self.register_data('cap', self.cap)
        self.register_data('vwap', self.vwap)
        self.register_data('adjfactor', self.accumAdjFactor)
        self.register_data('sharesout', self.sharesout)
        
        self.register_data('sector', self.sector)
        self.register_data('sectorIdx', self.sectorIdx)
        self.register_data('sectorName', self.sectorName)
        self.register_data('industry', self.industry)
        self.register_data('industryIdx', self.industryIdx)
        self.register_data('industryName', self.industryName)
        self.register_data('subindustry', self.subindustry)
        self.register_data('subindustryIdx', self.subindustryIdx)
        self.register_data('subindustryName', self.subindustryName)


    def caches(self):
        self.register_cache('isOpen')
        self.register_cache('CName')
        self.register_cache('open')
        self.register_cache('high')
        self.register_cache('low')
        self.register_cache('close')
        self.register_cache('volume')
        self.register_cache('amount')
        self.register_cache('turnover')
        self.register_cache('cap')
        self.register_cache('vwap')
        self.register_cache('accumAdjFactor')
        self.register_cache('sharesout')

        self.register_cache('sector')
        self.register_cache('sectorIdx')
        self.register_cache('sectorName')
        self.register_cache('industry')
        self.register_cache('industryIdx')
        self.register_cache('industryName')
        self.register_cache('subindustry')
        self.register_cache('subindustryIdx')
        self.register_cache('subindustryName')
        
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
                
    def dependencies(self):
        # Do not need dependencies in DataManagerBaseData
        pass
