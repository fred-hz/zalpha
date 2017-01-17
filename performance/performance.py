import numpy as np
from pipeline.module import DailyLoopModule
import math


class Performance(DailyLoopModule):

    def initialize(self):
        self.alpha = self.context.alpha
        self.long_capital = self.params['longcapital']
        self.short_capital = self.params['shortcapital']
        self.adj_cps = self.context.fetch_data('adj_close')
        self.adj_vwap = self.context.fetch_data('adj_vwap')
        self.start_di = self.context.date_idx(self.context.fetch_constant('startDate'))
        self.end_di = self.context.date_idx(self.context.fetch_constant('endDate'))
        self.di_size = len(self.context.di_list)
        self.ii_size = len(self.context.ii_list)
        self.history_position = np.zeros((self.di_size, self.ii_size))
        self.cumpnl = 0
        self.cumtvr = 0
        self.cumcapital = 0
        self.count = 0
        self.rmean = 0
        self.r2sum = 0
        self.alphaId = self.params['alpha_id']

        self.alpha = self.context.alpha

    def start_day(self, di):
        pass

    def intro_day(self, di):
        pass

    def end_day(self, di):
        if di > self.end_di or di < self.start_di:
            raise Exception('date error: beyond simulation date range!!!')

        self.count += 1
        long_num = np.sum(self.alpha[di] > 0)
        long_sum = np.sum(self.alpha[self.alpha > 0])
        short_num = np.sum(self.alpha[di] < 0)
        short_sum = np.sum(self.alpha[self.alpha < 0])
        if long_num == 0:
            raise Exception('Warning: no positive alpha value, cannot allocate long capital!!!')
        if short_num == 0:
            raise Exception('Warning: no negative alpha value, cannot allocate short capital!!!')
        if long_num > 0 and abs(long_sum) < 1e-5:
            raise Exception('Warning: long alpha value is too small!!!')
        if short_num > 0 and abs(short_sum) < 1e-5:
            raise Exception('Warning: short alpha value is too small!!!')

        self.history_position[di][self.alpha > 0] = self.long_capital * self.alpha[self.alpha > 0] / long_sum
        self.history_position[di][self.alpha < 0] = - self.short_capital * self.alpha[self.alpha < 0] / short_sum

        shares = np.zeros(self.ii_size)
        old_shares = np.zeros(self.ii_size)
        tmp = np.where(-np.isnan(self.adj_cps[di - 1]))[0]
        old_tmp = np.where(-np.isnan(self.adj_cps[di - 2]))[0]
        shares[tmp] = np.round(self.history_position[di][tmp] / self.adj_cps[di - 1][tmp])
        old_shares[old_tmp] = np.round(self.history_position[di - 1][old_tmp] / self.adj_cps[di - 2][old_tmp])

        total_shares = np.sum(np.absolute(shares))
        long_num = np.sum(shares > 0)
        short_num = np.sum(shares < 0)

        position = np.zeros(self.ii_size)
        old_position = np.zeros(self.ii_size)
        position[tmp] = shares[tmp] * self.adj_cps[di-1][tmp]
        old_position[old_tmp] = old_shares[old_tmp] * self.adj_cps[di-2][old_tmp]

        tvr = np.sum(np.absolute(position - old_position))
        self.cumtvr += tvr

        l_capital = np.sum(position[position > 0])
        s_capital = - np.sum(position[position < 0])
        self.cumcapital += l_capital + s_capital

        holding_pnl = np.nansum(old_shares * (self.adj_cps[di] - self.adj_cps[di-1]))
        trading_pnl = np.nansum((shares - old_shares) * (self.adj_cps[di] - self.adj_vwap[di]))
        pnl = holding_pnl + trading_pnl
        self.cumpnl += pnl

        ret = pnl / (l_capital + s_capital)
        if di == self.start_di:
            self.T = ret
            self.Vmin = ret
            self.Tmax = max(0, self.T)
        else:
            self.T += ret
            if self.T - self.Tmax < self.Vmin:
                self.Vmin = self.T - self.Tmax
            if self.T > self.Tmax:
                self.Tmax = self.T

        self.rmean = (self.rmean * (self.count-1) + ret) / self.count
        self.r2sum += pow(ret, 2)
        IR = self.rmean / math.sqrt(self.r2sum / self.count - pow(self.rmean, 2))
        date = self.context.di_list[di]
        print("%81s X %7s %10s %15s %25s %7s %7s %7s %7s" % ("LONG", "SHORT", "SHARES", "PNL", "CUMPNL", "TVR", "RET", "DD", "IR"))
        print("%8s %30s %15d X %15d %7d X %7d %10d %15d %25d %7.3f %7.3f %7.3f %7.3f" % (date, self.alphaId, int(l_capital), -int(s_capital), int(long_num), int(short_num), int(total_shares), int(pnl), int(self.cumpnl), self.cumtvr / self.cumcapital, self.cumpnl / self.cumcapital, min(0, self.Vmin) ,IR))
        with open('F:\zalpha\zalpha\pnl\\' + self.alphaId + '.csv', 'a') as output:
            output.write("%8s %15f %15f %15f %15f\n" % (date, pnl, tvr, l_capital, s_capital))

    def dependencies(self):
        self.register_dependency('adj_close')
        self.register_dependency('adj_vwap')


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
