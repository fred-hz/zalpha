"""
di_list is in the form of [20060101, 20060102, ...]
ii_list is in the form of ['000001', '000002', ...]
"""
di_list = []
ii_list = []

data_container = {}

def date_idx(date):
    """
    Get index for date in di_list
    :param date:
    :return:
    """
    return di_list.index(date)

def ticker_idx(ticker):
    """
    Get index for ticker in ii_list
    :param ticker:
    :return:
    """
    return ii_list.index(ticker)

def fetch_data(name):
    return data_container[name]