from pipeline.serialization import Serializable
import os, re

class EnvStandard(Serializable):

    def __init__(self, params):
        self.data_path = params['dataPath'] #路径和字符串
        self.cache_path = params['cachePath']
        self.start_date = params['startDate']
        self.end_date = params['endDate']

        super().__init__(self.cache_path)

        self.di_list = []
        self.ii_list = []
        self.add_cache()

    def add_cache(self):
        self.register_serialization('di_list')
        self.register_serialization('ii_list')

    def register_context(self, context):
        """
        Register standard environment into context
        :param context: sim_engine.context.Context
        :return:
        """
        context.di_list = self.di_list
        context.ii_list = self.ii_list
        
    def compute_cache(self):
        with open(os.path.join(self.data_path,'listing_date.csv')) as fp:
            content = fp.read().splitlines()

        for line in content[1:]:
            items = line.replace('"', '').replace('-', '').split(',')
            if items[2] == '1':
                self.di_list.append(items[1])

        address = self.data_path + '\\raw_stock_daily_data'
        files = os.listdir(address)

        for file_ in files:
            with open(address + '\\' + file_) as fp:
                content = fp.read().splitlines()
            for line in content[1:]:
                items = line.replace('"', '').split(',')
                ticker = re.sub('\D', '', items[1])
                if ticker not in self.ii_list:
                    self.ii_list.append(ticker)