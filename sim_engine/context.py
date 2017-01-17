import numpy as np

class Context(object):
    # Every test case need one singular context
    def __init__(self):
        """
        di_list is in the form of ['20060101', '20060102', ...]
        ii_list is in the form of ['000001', '000002', ...]
        """
        self.di_list = None
        self.ii_list = None

        self.data_container = {
        }

        # Record if specific data is loaded
        self.data_loaded = {
        }

        self.constants = {}

        self.start_date = None
        self.end_date = None
        self.start_di = None
        self.end_di = None

        self.is_valid = None

    def shallow_copy(self):
        result = Context()
        for item in self.__dict__.keys():
            if item == 'is_valid':
                setattr(result, item, None)
            else:
                setattr(result, item, getattr(self, item))
        return result

    def set_shape(self, ii_size):
        self.alpha = np.zeros(ii_size, dtype='float')
        self.alpha.flat = np.nan

    def set_di_list(self, _list):
        self.di_list = _list

    def set_ii_list(self, _list):
        self.ii_list = _list

    def date_idx(self, date):
        """
        Get index for date in di_list
        :param date:
        :return:
        """
        return self.di_list.index(date)

    def ticker_idx(self, ticker):
        """
        Get index for ticker in ii_list
        :param ticker:
        :return:
        """
        return self.ii_list.index(ticker)

    def register_data(self, name, data):
        self.data_container[name] = data

    def fetch_data(self, name):
        return self.data_container[name]

    def has_data(self, name):
        return name in self.data_loaded.keys() and self.data_loaded[name] is True

    def mark_loaded_data(self, name):
        self.data_loaded[name] = True

    def register_constant(self, name, value):
        self.constants[name] = value

    def fetch_constant(self, name):
        return self.constants[name]