from alpha.alpha_base import AlphaBase

class AlphaSample(AlphaBase):
    def initialize(self):
        self.delay = int(self.params['delay'])
        self.is_valid = self.context.fetch_dat('is_valid')
        self.cps = self.context.fetch_data('adj_close')

    def compute_day(self, di):
        for ii in range(len(self.context.ii_list)):
            if self.is_valid[di][ii]:
                self.alpha[ii] = self.cps[di - self.delay - 5] - self.cps[di - self.delay]

    def dependencies(self):
        self.register_dependency('adj_close')
        self.register_dependency('is_valid')
