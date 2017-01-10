from alpha.alpha_base import AlphaBase

class AlphaSample(AlphaBase):
    def dependencies(self):
        self.register_dependency('open')
        self.register_dependency('high')

    def initialize(self):
        self.open = self.context.fetch_data('open')
        self.high = self.context.fetch_data('high')
        self.is_valid = self.context.fetch_data9('is_valid')

    def compute_day(self, di):
        for ii in range(0, len(self.context.ii_list)):
            # Universe 还有问题，先假设直接用 self.is_valid
            if self.is_valid[ii]:
                self.alpha[ii] = 0
