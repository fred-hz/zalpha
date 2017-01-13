import numpy as np
from pipeline.module import DailyLoopModule

class Performance(DailyLoopModule):

    def start_day(self, di):
        pass

    def end_day(self, di):
        self.daily_stats(self.context.alpha, di)

    def intro_day(self, di):
        pass

    def dependencies(self):
        self.register_dependency('adj_close')
        self.register_dependency('adj_vwap')
        self.register_dependency('ZZ500')

    def initialize(self):
        self.adj_close = self.context.fetch_data('adj_close')
        self.adj_vwap = self.context.fetch_data('adj_vwap')

        self.long_mode = self.params['long_mode']
        self.short_mode = self.params['short_mode']
        self.long_capital = self.params['long_capital']
        self.short_capital = self.params['short_capital']
        self.tax = self.params['tax']
        self.fee = self.params['fee']

        di_size = len(self.context.di_list)
        ii_size = len(self.context.ii_list)
        self.history_position = np.ndarray((di_size, ii_size))

    def daily_stats(self, alpha, di):
        pass

"""
Alpha based functions should return in the form of
{
    'type': 'alpha_based',
    'pnl': xxx,
    'long_capital': xxx,
    'short_capital': xxx,
    'long_num': xxx,
    'short_num': xxx,
    'tvr': xxx
}
di: date index
alpha: alpha values for each stock on di
is_long: True for long and False for short
capital: capitals to be positioned
context: prices data for calculating positions
history_position: capital position for each stock in history
start_di: di of the start date of backtest
end_di: di of the last date of backtest
threhold: only deal with the top n stocks
"""

# def alpha_normal(alpha, di, is_long, capital, context, history_position, start_di, end_di):
#     if di > end_di or di < start_di:
#         raise Exception('date error: beyond simulation date range!!!')
#     adj_cps = context.fetch_data('adjust_close')
#     adj_vwap = context.fetch_data('adjust_vwap')
#     result = {'type': 'alpha_based'}
#     if is_long:
#         num = np.sum(alpha > 0)
#         sum = np.sum(alpha[alpha > 0])
#     else:
#         num = np.sum(alpha < 0)
#         sum = np.sum(alpha[alpha < 0])
#     if num == 0:
#         if is_long:
#             raise Exception('there is no positive alpha value, so we cannot allocate long capital!!!')
#         else:
#             raise Exception('there is no negative alpha value, so we cannot allocate short capital!!!')
#     if num > 0 and abs(sum) < 1e-5:
#         raise Exception('alpha value is too small!!!')
#
#     if is_long:
#         history_position[di][alpha > 0] = capital * alpha[alpha > 0] /sum
#     else:
#         history_position[di][alpha < 0] = -capital * alpha[alpha < 0] / sum
#
#     if di == start_di:
#         pnl = 0.
#         tvr = capital
#     else:
#         if is_long:
#             tvr = np.sum(np.absolute(history_position[di][alpha > 0] - history_position[di - 1][alpha > 0]))
#             tmp = np.where(alpha > 0 & -np.isnan(adj_cps[di - 2]) & -np.isnan(adj_cps[di - 1]) & -np.isnan(adj_cps[di]) & -np.isnan(adj_vwap[di]))[0]
#             pnl = np.sum(history_position[di][tmp] / adj_cps[di - 1][tmp] * (adj_cps[di][tmp] - adj_vwap[di][tmp]) + \
#                          history_position[di-1][tmp] / adj_cps[di - 2][tmp] * (adj_vwap[di][tmp] - adj_cps[di - 1][tmp]))
#         else:
#             tvr = np.sum(np.absolute(history_position[di][alpha < 0] - history_position[di - 1][alpha < 0]))
#             tmp = np.where(alpha < 0 & -np.isnan(adj_cps[di - 2]) & -np.isnan(adj_cps[di - 1]) & -np.isnan(adj_cps[di]) & -np.isnan(adj_vwap[di]))[0]
#             pnl = np.sum(history_position[di][tmp] / adj_cps[di - 1][tmp] * (adj_cps[di][tmp] - adj_vwap[di][tmp]) + \
#                          history_position[di - 1][tmp] / adj_cps[di - 2][tmp] * (adj_vwap[di][tmp] - adj_cps[di - 1][tmp]))
#
#     result['pnl'] = pnl
#     if is_long:
#         result['long_capital'] = capital
#         result['short_capital'] = 0
#         result['long_num'] = num
#         result['short_num'] = 0
#     else:
#         result['long_capital'] = 0
#         result['short_capital'] = capital
#         result['long_num'] = 0
#         result['short_num'] = num
#     result['tvr'] = tvr
#     return result
#
#
# def alpha_topN(alpha, di, is_long, capital, context, history_position, start_di, end_di, thredhold = 50):
#     pass
#
# """
# Index based functions should return in the form of
# {
#     'type': 'index_based',
#     'pnl': xxx,
#     'long_capital': xxx,
#     'short_capital': xxx,
#     'tvr': xxx
# }
# IC: ZZ500
# IF: HS300
# IH: SZ50
# """
#
# def index_normal(di, is_long, capital, index = 'IC'):
#     pass
#
# alpha_based_mapping ={
#     'alpha_normal': alpha_normal,
#     'alpha_topN': alpha_topN
# }
#
# index_based_mapping = {
#     'index_normal': index_normal,
# }
#
#
# class Performance(Module):
#     def __init__(self, params, context):
#         pass
#
#     # def __init__(self, context, start_di, end_di, long_mode, short_mode, long_capital, short_capital):
#     #     self.context = context
#     #     self.start_di = start_di
#     #     self.end_di = end_di
#     #     self.long_mode = long_mode
#     #     self.short_mode = short_mode
#     #     self.long_capital = long_capital
#     #     self.short_capital = short_capital
#     #
#     #     di_size = len(self.context.di_list)
#     #     ii_size = len(self.context.ii_list)
#     #     # Stores the capital position of each stock
#     #     self.alpha_positions = np.zeros((di_size, ii_size))
#     #
#     #     # Stores all the history stats
#     #     self.history_stats = []
#
#     def initialize(self):
#         pass
#
#     def compute_day(self, di):
#         pass
#
#     def daily_stats(self, alpha, di):
#         long_stats = None
#         short_stats = None
#
#         if self.long_mode in alpha_based_mapping.keys():
#             long_stats = alpha_based_mapping[self.long_mode](alpha=alpha,
#                                                              di=di,
#                                                              is_long=True,
#                                                              capital=self.long_capital,
#                                                              context=self.context,
#                                                              history_position=self.alpha_positions,
#                                                              start_di=self.start_di,
#                                                              end_di=self.end_di)
#         elif self.long_mode in index_based_mapping.keys():
#             long_stats = index_based_mapping[self.long_mode](di=di,
#                                                              is_long=True,
#                                                              capital=self.long_capital,
#                                                              index='')
#         else:
#             raise Exception
#
#         if self.short_mode in alpha_based_mapping.keys():
#             short_stats = alpha_based_mapping[self.short_mode](alpha=alpha,
#                                                                di=di,
#                                                                is_long=False,
#                                                                capital=self.short_capital,
#                                                                context=self.context,
#                                                                history_position=self.alpha_positions,
#                                                                start_di=self.start_di,
#                                                                end_di=self.end_di)
#         elif self.short_mode in index_based_mapping.keys():
#             short_stats = index_based_mapping[self.short_mode](di=di,
#                                                                is_long=False,
#                                                                capital=self.short_capital,
#                                                                index='')
#         else:
#             raise Exception
#
#         # to-do: combine the long and short stats. Then store it to self.history_stats
#
