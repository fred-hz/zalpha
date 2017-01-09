class Context(object):
    # Every test case need one singular context

    def __init__(self):
        """
        di_list is in the form of ['20060101', '20060102', ...]
        ii_list is in the form of ['000001', '000002', ...]
        """
        self.di_list = []
        self.ii_list = []
        self.data_container = {
            'is_valid': None
        }

        self.constants = {}

        self.start_date = None
        self.end_date = None
        self.start_di = None
        self.end_di = None

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

    def register_constant(self, name, value):
        self.constants[name] = value

    def fetch_constant(self, name):
        return self.constants[name]