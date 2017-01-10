from data_manager.dm_base import DataManagerBase
import numpy as np
import re, os


class DataManagerBaseData(DataManagerBase):

    def __init__(self, params, context):
        super(DataManagerBaseData, self).__init__(params=params, context=context)
        # Fetch data from context and identify values to variables
        self.data_path = self.params['dataPath']
        self.sector_path = self.params['sectorPath']
        self.ticker_list = self.context.ii_list
        self.date_list = self.context.di_list

    def provide_data(self):
        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)

        self._CName = []
        for di in range(di_size):
            temp = []
            for ii in range(ii_size):
                temp.append('nan')
            self._CName.append(temp)

        self._isOpen = np.ndarray((di_size, ii_size), dtype=float)
        self._open = np.ndarray((di_size, ii_size), dtype=float)
        self._high = np.ndarray((di_size, ii_size), dtype=float)
        self._low = np.ndarray((di_size, ii_size), dtype=float)
        self._close = np.ndarray((di_size, ii_size), dtype=float)
        self._volume = np.ndarray((di_size, ii_size), dtype=float)
        self._amount = np.ndarray((di_size, ii_size), dtype=float)
        self._turnover = np.ndarray((di_size, ii_size), dtype=float)
        self._cap = np.ndarray((di_size, ii_size), dtype=float)  # 流通市值
        self._vwap = np.ndarray((di_size, ii_size), dtype=float)
        self._accumAdjFactor = np.ndarray((di_size, ii_size), dtype=float)
        self._sharesout = np.ndarray((di_size, ii_size), dtype=float)  # 流通股数

        self._sector = np.ndarray((di_size, ii_size), dtype=float)
        self._sectorIdx = []
        self._sectorName = []
        self._industry = np.ndarray((di_size, ii_size), dtype=float)
        self._industryIdx = []
        self._industryName = []
        self._subindustry = np.ndarray((di_size, ii_size), dtype=float)
        self._subindustryIdx = []
        self._subindustryName = []

        self.register_data('isOpen', self._isOpen)
        self.register_data('CName', self._CName)
        self.register_data('open', self._open)
        self.register_data('high', self._high)
        self.register_data('low', self._low)
        self.register_data('close', self._close)
        self.register_data('volume', self._volume)
        self.register_data('amount', self._amount)
        self.register_data('turnover', self._turnover)
        self.register_data('cap', self._cap)
        self.register_data('vwap', self._vwap)
        self.register_data('adjfactor', self._accumAdjFactor)
        self.register_data('sharesout', self._sharesout)
        
        self.register_data('sector', self._sector)
        self.register_data('sectorIdx', self._sectorIdx)
        self.register_data('sectorName', self._sectorName)
        self.register_data('industry', self._industry)
        self.register_data('industryIdx', self._industryIdx)
        self.register_data('industryName', self._industryName)
        self.register_data('subindustry', self._subindustry)
        self.register_data('subindustryIdx', self._subindustryIdx)
        self.register_data('subindustryName', self._subindustryName)


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
        date = self.date_list[di]
        print("Begin compute basedata: " + date)
        try:
            with open(self.data_path + '\\' + date + '.csv', 'r') as fp:
                content = fp.read().splitlines()
        except IOError:
            print("Warning: " + date + " price file is missing")
        else:
            for line in content[1:]:
                items = line.replace('"', '').split(',')
                ticker = re.sub('\D', '', items[1])
                # print(ticker)
                ii = self.ticker_list.index(ticker)
                # print(ii)
                self._isOpen[di][ii] = float(items[-1])
                if self._isOpen[di][ii] > 0.5:
                    if not items[7] == '':
                        if float(items[7]) < 1e-5:
                            print(
                                "warning : there is abnormal value : open of ticker : " + ticker + " at date: " + date)
                        self._open[di][ii] = float(items[7])
                    else:
                        print("warning : there is no open price of ticker : " + ticker + " at date: " + date)
                        self._open[di][ii] = np.nan
                    if not items[8] == '':
                        if float(items[8]) < 1e-5:
                            print(
                                "warning : there is abnormal value : high of ticker : " + ticker + " at date: " + date)
                        self._high[di][ii] = float(items[8])
                    else:
                        print("warning : there is no high price of ticker : " + ticker + " at date: " + date)
                        self._high[di][ii] = np.nan
                    if not items[9] == '':
                        if float(items[9]) < 1e-5:
                            print(
                                "warning : there is abnormal value : low of ticker : " + ticker + " at date: " + date)
                        self._low[di][ii] = float(items[9])
                    else:
                        print("warning : there is no low price of ticker : " + ticker + " at date: " + date)
                        self._low[di][ii] = np.nan
                    if not items[10] == '':
                        if float(items[10]) < 1e-5:
                            print(
                                "warning : there is abnormal value : close of ticker : " + ticker + " at date: " + date)
                        self._close[di][ii] = float(items[10])
                    else:
                        print("warning : there is no close price of ticker : " + ticker + " at date: " + date)
                        self._close[di][ii] = np.nan
                    if not items[11] == '':
                        if float(items[11]) < 1e-5:
                            print(
                                "warning : there is abnormal value : volume of ticker : " + ticker + " at date: " + date)
                        self._volume[di][ii] = float(items[11])
                    else:
                        print("warning : there is no volume price of ticker : " + ticker + " at date: " + date)
                        self._volume[di][ii] = np.nan
                    if not items[12] == '':
                        if float(items[12]) < 1e-5:
                            print(
                                "warning : there is abnormal value : vwap of ticker : " + ticker + " at date: " + date)
                        self._vwap[di][ii] = float(items[12]) / self._volume[di][ii]
                    else:
                        print("warning : there is no vwap price of ticker : " + ticker + " at date: " + date)
                        self._vwap[di][ii] = np.nan
                    if not items[13] == '':
                        if float(items[13]) < 1e-5:
                            print(
                                "warning : there is abnormal value : amount of ticker : " + ticker + " at date: " + date)
                        self._amount[di][ii] = float(items[13])
                    else:
                        # print("warning : there is no amount price of ticker : " + ticker + " at date: " + date)
                        self._amount[di][ii] = np.nan
                    if not items[16] == '':
                        if float(items[16]) < 1e-5:
                            print(
                                "warning : there is abnormal value : cap of ticker : " + ticker + " at date: " + date)
                        self._cap[di][ii] = float(items[16])
                    else:
                        print("warning : there is no cap price of ticker : " + ticker + " at date: " + date)
                        self._cap[di][ii] = np.nan
                    self._sharesout[di][ii] = self._cap[di][ii] / self._close[di][ii]
                    self._turnover[di][ii] = self._volume[di][ii] / self._sharesout[di][ii]
                else:
                    self._open[di][ii] = np.nan
                    self._high[di][ii] = np.nan
                    self._low[di][ii] = np.nan
                    self._close[di][ii] = np.nan
                    self._volume[di][ii] = np.nan
                    self._vwap[di][ii] = np.nan
                    self._amount[di][ii] = np.nan
                    self._turnover[di][ii] = np.nan
                    self._cap[di][ii] = np.nan
                    self._sharesout[di][ii] = np.nan

                if not items[2] == '':
                    self._CName[di][ii] = items[2]
                else:
                    print("warning : there is no company name data of ticker : " + ticker + " at date: " + date)

                if not items[15] == '':
                    self._accumAdjFactor[di][ii] = float(items[15])
                else:
                    print(
                        "warning : there is no accumulated adjust factor data of ticker : " + ticker + " at date: " + date)
                    self._accumAdjFactor[di][ii] = np.nan

        try:
            with open(self.sector_path + '\\' + date + '.csv') as fp:
                content = fp.read().splitlines()
        except IOError:
            print("Warning: " + date + " sector file is missing")
        else:
            if di > 0:
                self._sector[di] = self._sector[di - 1]
                self._industry[di] = self._industry[di - 1]
                self._subindustry[di] = self._subindustry[di - 1]

            for line in content[1:]:
                items = line.replace('"', '').split(',')
                ticker = re.sub('\D', '', items[1])
                try:
                    ii = self.ticker_list.index(ticker)
                except ValueError:
                    # print('Warning: ' + ticker + ' is not in the ticker list')
                    continue
                sectorSymbol = items[9][:2]
                mark = -1
                for i in range(len(self._sectorIdx)):
                    if self._sectorIdx[i] == sectorSymbol:
                        mark = i
                        break

                if mark == -1:
                    mark = len(self._sectorIdx)
                    self._sectorIdx.append(sectorSymbol)
                    self._sectorName.append(items[13])
                self._sector[di][ii] = mark

                industrySymbol = items[9][:4]
                mark = -1
                for i in range(len(self._industryIdx)):
                    if self._industryIdx[i] == industrySymbol:
                        mark = i
                        break

                if mark == -1:
                    mark = len(self._industryIdx)
                    self._industryIdx.append(industrySymbol)
                    self._industryName.append(items[15])
                self._industry[di][ii] = mark

                subindustrySymbol = items[9]
                mark = -1
                for i in range(len(self._subindustryIdx)):
                    if self._subindustryIdx[i] == subindustrySymbol:
                        mark = i
                        break

                if mark == -1:
                    mark = len(self._subindustryIdx)
                    self._subindustryIdx.append(subindustrySymbol)
                    self._subindustryName.append(items[17])
                self._subindustry[di][ii] = mark

        print('Finish build basedata: ' + date)

    def dependencies(self):
        # Do not need dependencies in DataManagerBaseData
        pass
