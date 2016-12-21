import numpy as np
import sim_engine.context as Context

"""
Alpha based functions should return in the form of
{
    'type': 'alpha_based',
    'pnl': xxx,
    'long_capital': xxx,
    'short_capital': xxx,
    'tvr': xxx,
    'sr': xxx,
    'ir': xxx
}
di: date index
alpha: alpha values for each stock on di
is_long: True for long and False for short
capital: capitals to be positioned
price: deal price for each stock on di
history_position: capital position for each stock in history
start_di: di of the start date of back test
"""
def alpha_normal(alpha, di, is_long, capital, price, history_position, start_di):
    pass

def alpha_top50(alpha, di, is_long, capital, price, history_position, start_di):
    pass

"""
Index based functions only return pnl.
{
    'type': 'alpha_based',
    'pnl': xxx,
    'long_capital': xxx,
    'short_capital': xxx
}
"""
def index_ZZ500(di, is_long, capital, price):
    pass


alpha_based_mapping ={
    'alpha_normal_long': alpha_normal,
    'alpha_top50': alpha_top50
}

index_based_mapping = {
    'index_ZZ500': index_ZZ500
}


class Performance(object):
    def __init__(self, start_di, end_di, long_mode, short_mode, long_capital, short_capital, ticker_price):
        self.start_di = start_di
        self.end_di = end_di
        self.long_mode = long_mode
        self.short_mode = short_mode
        self.long_capital = long_capital
        self.short_capital = short_capital
        # Deal price: open, close or vwap. used in the form of ticker_price[di][ii]
        self.ticker_price = ticker_price

        di_size = len(Context.di_list)
        ii_size = len(Context.ii_list)
        # Stores the capital position of each stock
        self.alpha_positions = np.zeros((di_size, ii_size))

        # Stores all the history stats
        self.history_stats = []

    def daily_stats(self, alpha, di):
        long_stats = None
        short_stats = None

        if self.long_mode in alpha_based_mapping.keys():
            long_stats = alpha_based_mapping[self.long_mode](alpha=alpha,
                                                             di=di,
                                                             is_long=True,
                                                             capital=self.long_capital,
                                                             price=self.ticker_price,
                                                             history_position=self.alpha_positions,
                                                             start_di=self.start_di)
        elif self.long_mode in index_based_mapping.keys():
            long_stats = index_based_mapping[self.long_mode](di=di,
                                                             is_long=True,
                                                             capital=self.long_capital,
                                                             price=Context.data_container[self.long_mode])
        else:
            raise Exception

        if self.short_mode in alpha_based_mapping.keys():
            short_stats = alpha_based_mapping[self.short_mode](alpha=alpha,
                                                               di=di,
                                                               is_long=False,
                                                               capital=self.short_capital,
                                                               price=self.ticker_price,
                                                               history_position=self.alpha_positions,
                                                               start_di=self.start_di)
        elif self.short_mode in index_based_mapping.keys():
            short_stats = index_based_mapping[self.short_mode](di=di,
                                                               is_long=False,
                                                               capital=self.short_capital,
                                                               price=Context.data_container[self.long_mode])
        else:
            raise Exception

        # to-do: combine the long and short stats. Then store it to self.history_stats

