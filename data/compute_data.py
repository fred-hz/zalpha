from dataapi import Client
from time import gmtime, strftime
import os, pickle, re
import numpy as np

class Production:
    def __init__(self, params):
        self.data_path = params['data_path']
        self.date_list = []
        self.ticker_list = []

    def download_data(self):
        try:
            client = Client()
            client.init('e06a5d74629c1d3f764ad0af425b5e316421b83ba7f3111e8bb8789aeb216507')

            url = '/api/master/getTradeCal.csv?field=&exchangeCD=XSHG&beginDate=20060101&endDate=' + strftime("%Y%m%d", gmtime())
            code, result = client.getData(url)
            if code == 200:
                with open('listing_date.csv', 'wb') as file_object:
                    file_object.write(result)
            else:
                print(code)
                print(result)

            print('Begin calculate date list')
            content = result.decode().splitlines()
            for line in content[1:]:
                items = line.split(',')
                if items[2] == '1':
                    self.date_list.append(items[1][1:5] + items[1][6:8] + items[1][9:11])

            with open(data_path + '\\cache\\date_list.dat', 'wb') as output:
                pickle.dump(self.date_list, output)
            print('Finish dump date list')

            file_list = [file[:8] for file in os.listdir(os.path.join(self.data_path, 'raw_stock_daily_data'))]
            file_list_SW = [file[:8] for file in os.listdir(os.path.join(self.data_path, 'raw_sector_daily_data\SW'))]
            file_list_ZJ = [file[:8] for file in os.listdir(os.path.join(self.data_path, 'raw_sector_daily_data\ZJ'))]
            file_list_ZZ = [file[:8] for file in os.listdir(os.path.join(self.data_path, 'raw_sector_daily_data\ZZ'))]
            for date in self.date_list:
                if date not in file_list:
                    url = '/api/market/getMktEqud.csv?field=&beginDate=&endDate=&secID=&ticker=&tradeDate=' + date
                    code, result = client.getData(url)
                    if code == 200:
                        address = os.path.join(self.data_path, 'raw_stock_daily_data')
                        with open(address + '\\' + date + '.csv', 'wb') as file_object:
                            file_object.write(result)
                    else:
                        print(code)
                        print(result)
                if date not in file_list_SW:
                    url = '/api/equity/getEquIndustry.csv?field=&industryVersionCD=010303&industry=&secID=&ticker=&intoDate=' + date
                    code, result = client.getData(url)
                    if code == 200:
                        address = os.path.join(self.data_path, 'raw_sector_daily_data\SW')
                        with open(address + '\\' + date + '.csv', 'wb') as file_object:
                            file_object.write(result)
                    else:
                        print(code)
                        print(result)
                if date not in file_list_ZJ:
                    url = '/api/equity/getEquIndustry.csv?field=&industryVersionCD=010301&industry=&secID=&ticker=&intoDate=' + date
                    code, result = client.getData(url)
                    if code == 200:
                        address = os.path.join(self.data_path, 'raw_sector_daily_data\ZJ')
                        with open(address + '\\' + date + '.csv', 'wb') as file_object:
                            file_object.write(result)
                    else:
                        print(code)
                        print(result)
                if date not in file_list_ZZ:
                    url = '/api/equity/getEquIndustry.csv?field=&industryVersionCD=010308&industry=&secID=&ticker=&intoDate=' + date
                    code, result = client.getData(url)
                    if code == 200:
                        address = os.path.join(self.data_path, 'raw_sector_daily_data\ZZ')
                        with open(address + '\\' + date + '.csv', 'wb') as file_object:
                            file_object.write(result)
                    else:
                        print(code)
                        print(result)
        except Exception as e:
            # traceback.print_exc()
            raise e

    def compute_basedata(self):
        address = os.path.join(self.data_path, 'raw_stock_daily_data')
        files = os.listdir(address)

        print("Begin calculate ticker list")
        for file_ in files:
            with open(address + '\\' + file_) as fp:
                content = fp.read().splitlines()
            for line in content[1:]:
                items = line.replace('"', '').split(',')
                ticker = re.sub('\D', '', items[1])
                if ticker not in self.ticker_list:
                    self.ticker_list.append(ticker)

        with open(self.data_path + '\\cache\\ticker_list.dat', 'wb') as output:
            pickle.dump(self.ticker_list, output)
        print("Finish dump ticker list")

        di_size = len(self.date_list)
        ii_size = len(self.ticker_list)

        print('ticker list has '+str(ii_size)+' tickers')

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
        _amount = np.ndarray((di_size, ii_size), dtype=float)
        _turnover = np.ndarray((di_size, ii_size), dtype=float)
        _cap = np.ndarray((di_size, ii_size), dtype=float)  # 流通市值
        _vwap = np.ndarray((di_size, ii_size), dtype=float)
        _accumAdjFactor = np.ndarray((di_size, ii_size), dtype=float)
        _sharesout = np.ndarray((di_size, ii_size), dtype=float)  # 流通股数

        _sector = np.ndarray((di_size, ii_size), dtype=float)
        _sectorIdx = []
        _sectorName = []
        _industry = np.ndarray((di_size, ii_size), dtype=float)
        _industryIdx = []
        _industryName = []
        _subindustry = np.ndarray((di_size, ii_size), dtype=float)
        _subindustryIdx = []
        _subindustryName = []


        for di in range(len(date_list)):
            date = date_list[di]
            print("Begin compute basedata: " + date)
            try:
                with open(address + '\\' + date + '.csv', 'r') as fp:
                    content = fp.read().splitlines()
            except IOError:
                print("Warning: " + date + " price file is missing")
            else:
                for line in content[1:]:
                    items = line.replace('"', '').split(',')
                    ticker = re.sub('\D', '', items[1])
                    #print(ticker)
                    ii = ticker_list.index(ticker)
                    #print(ii)
                    self._isOpen[di][ii] = float(items[-1])
                    if self._isOpen[di][ii] > 0.5:
                        if not items[7] == '':
                            if float(items[7]) < 1e-5:
                                print("warning : there is abnormal value : open of ticker : " + ticker + " at date: " + date)
                            self._open[di][ii] = float(items[7])
                        else:
                            print("warning : there is no open price of ticker : " + ticker + " at date: " + date)
                            self._open[di][ii] = np.nan
                        if not items[8] == '':
                            if float(items[8]) < 1e-5:
                                print("warning : there is abnormal value : high of ticker : " + ticker + " at date: " + date)
                            self._high[di][ii] = float(items[8])
                        else:
                            print("warning : there is no high price of ticker : " + ticker + " at date: " + date)
                            self._high[di][ii] = np.nan
                        if not items[9] == '':
                            if float(items[9]) < 1e-5:
                                print("warning : there is abnormal value : low of ticker : " + ticker + " at date: " + date)
                            self._low[di][ii] = float(items[9])
                        else:
                            print("warning : there is no low price of ticker : " + ticker + " at date: " + date)
                            self._low[di][ii] = np.nan
                        if not items[10] == '':
                            if float(items[10]) < 1e-5:
                                print("warning : there is abnormal value : close of ticker : " + ticker + " at date: " + date)
                            self._close[di][ii] = float(items[10])
                        else:
                            print("warning : there is no close price of ticker : " + ticker + " at date: " + date)
                            self._close[di][ii] = np.nan
                        if not items[11] == '':
                            if float(items[11]) < 1e-5:
                                print("warning : there is abnormal value : volume of ticker : " + ticker + " at date: " + date)
                            self._volume[di][ii] = float(items[11])
                        else:
                            print("warning : there is no volume price of ticker : " + ticker + " at date: " + date)
                            self._volume[di][ii] = np.nan
                        if not items[12] == '':
                            if float(items[12]) < 1e-5:
                                print("warning : there is abnormal value : vwap of ticker : " + ticker + " at date: " + date)
                            _vwap[di][ii] = float(items[12]) / self._volume[di][ii]
                        else:
                            print("warning : there is no vwap price of ticker : " + ticker + " at date: " + date)
                            _vwap[di][ii] = np.nan
                        if not items[13] == '':
                            if float(items[13]) < 1e-5:
                                print("warning : there is abnormal value : amount of ticker : " + ticker + " at date: " + date)
                            _amount[di][ii] = float(items[13])
                        else:
                            #print("warning : there is no amount price of ticker : " + ticker + " at date: " + date)
                            _amount[di][ii] = np.nan
                        if not items[16] == '':
                            if float(items[16]) < 1e-5:
                                print("warning : there is abnormal value : cap of ticker : " + ticker + " at date: " + date)
                            _cap[di][ii] = float(items[16])
                        else:
                            print("warning : there is no cap price of ticker : " + ticker + " at date: " + date)
                            _cap[di][ii] = np.nan
                        _sharesout[di][ii] = _cap[di][ii] / self._close[di][ii]
                        _turnover[di][ii] = self._volume[di][ii] / _sharesout[di][ii]
                    else:
                        self._open[di][ii] = np.nan
                        self._high[di][ii] = np.nan
                        self._low[di][ii] = np.nan
                        self._close[di][ii] = np.nan
                        self._volume[di][ii] = np.nan
                        _vwap[di][ii] = np.nan
                        _amount[di][ii] = np.nan
                        _turnover[di][ii] = np.nan
                        _cap[di][ii] = np.nan
                        _sharesout[di][ii] = np.nan

                    if not items[2] == '':
                        self._CName[di][ii] = items[2]
                    else:
                        print("warning : there is no company name data of ticker : " + ticker + " at date: " + date)

                    if not items[15] == '':
                        _accumAdjFactor[di][ii] = float(items[15])
                    else:
                        print("warning : there is no accumulated adjust factor data of ticker : " + ticker + " at date: " + date)
                        _accumAdjFactor[di][ii] = np.nan

            try:
                with open(os.path.join(data_path, 'raw_sector_daily_data\SW') + '\\' + date + '.csv') as fp:
                    content = fp.read().splitlines()
            except IOError:
                print("Warning: " + date + " sector file is missing")
            else:
                if di > 0:
                    _sector[di] = _sector[di - 1]
                    _industry[di] = _industry[di - 1]
                    _subindustry[di] = _subindustry[di - 1]

                for line in content[1:]:
                    items = line.replace('"', '').split(',')
                    ticker = re.sub('\D', '', items[1])
                    try:
                        ii = ticker_list.index(ticker)
                    except ValueError:
                        #print('Warning: ' + ticker + ' is not in the ticker list')
                        continue
                    sectorSymbol = items[9][:2]
                    mark = -1
                    for i in range(len(_sectorIdx)):
                        if _sectorIdx[i] == sectorSymbol:
                            mark = i
                            break

                    if mark == -1:
                        mark = len(_sectorIdx)
                        _sectorIdx.append(sectorSymbol)
                        _sectorName.append(items[13])
                    _sector[di][ii] = mark

                    industrySymbol = items[9][:4]
                    mark = -1
                    for i in range(len(_industryIdx)):
                        if _industryIdx[i] == industrySymbol:
                            mark = i
                            break

                    if mark == -1:
                        mark = len(_industryIdx)
                        _industryIdx.append(industrySymbol)
                        _industryName.append(items[15])
                    _industry[di][ii] = mark

                    subindustrySymbol = items[9]
                    mark = -1
                    for i in range(len(_subindustryIdx)):
                        if _subindustryIdx[i] == subindustrySymbol:
                            mark = i
                            break

                    if mark == -1:
                        mark = len(_subindustryIdx)
                        _subindustryIdx.append(subindustrySymbol)
                        _subindustryName.append(items[17])
                    _subindustry[di][ii] = mark

        with open(data_path + '\\cache\\basedata\\CName.dat', 'wb') as output:
            pickle.dump(self._CName, output)
        with open(data_path + '\\cache\\basedata\\isOpen.dat', 'wb') as output:
            pickle.dump(self._isOpen, output)
        with open(data_path + '\\cache\\basedata\\open.dat', 'wb') as output:
            pickle.dump(self._open, output)
        with open(data_path + '\\cache\\basedata\\high.dat', 'wb') as output:
            pickle.dump(self._high, output)
        with open(data_path + '\\cache\\basedata\\low.dat', 'wb') as output:
            pickle.dump(self._low, output)
        with open(data_path + '\\cache\\basedata\\close.dat', 'wb') as output:
            pickle.dump(self._close, output)
        with open(data_path + '\\cache\\basedata\\volume.dat', 'wb') as output:
            pickle.dump(self._volume, output)
        with open(data_path + '\\cache\\basedata\\amount.dat', 'wb') as output:
            pickle.dump(_amount, output)
        with open(data_path + '\\cache\\basedata\\turnover.dat', 'wb') as output:
            pickle.dump(_turnover, output)
        with open(data_path + '\\cache\\basedata\\cap.dat', 'wb') as output:
            pickle.dump(_cap, output)
        with open(data_path + '\\cache\\basedata\\vwap.dat', 'wb') as output:
            pickle.dump(_vwap, output)
        with open(data_path + '\\cache\\basedata\\accumAdjFactor.dat', 'wb') as output:
            pickle.dump(_accumAdjFactor, output)
        with open(data_path + '\\cache\\basedata\\sharesout.dat', 'wb') as output:
            pickle.dump(_sharesout, output)
        with open(data_path + '\\cache\\basedata\\sector.dat', 'wb') as output:
            pickle.dump(_sector, output)
        with open(data_path + '\\cache\\basedata\\sectorIdx.dat', 'wb') as output:
            pickle.dump(_sectorIdx, output)
        with open(data_path + '\\cache\\basedata\\sectorName.dat', 'wb') as output:
            pickle.dump(_sectorName, output)
        with open(data_path + '\\cache\\basedata\\industry.dat', 'wb') as output:
            pickle.dump(_industry, output)
        with open(data_path + '\\cache\\basedata\\industryIdx.dat', 'wb') as output:
            pickle.dump(_industryIdx, output)
        with open(data_path + '\\cache\\basedata\\industryName.dat', 'wb') as output:
            pickle.dump(_industryName, output)
        with open(data_path + '\\cache\\basedata\\subindustry.dat', 'wb') as output:
            pickle.dump(_subindustry, output)
        with open(data_path + '\\cache\\basedata\\subindustryIdx.dat', 'wb') as output:
            pickle.dump(_subindustryIdx, output)
        with open(data_path + '\\cache\\basedata\\subindustryName.dat', 'wb') as output:
            pickle.dump(_subindustryName, output)

        print('Finish dump basedata')



data_path = 'F:\zalpha\zalpha\data'