from pipeline.serialization import Serializable

class EnvStandard(Serializable):

    def __init__(self, paras):
        super().__init__()
        self.data_path = paras['dataPath']
        self.cache_path = paras['cachePath']
        self.start_date = paras['startDate']
        self.end_date = paras['endDate']

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

    def compute(self):
        pass

    def load_data(self):
        if self.cache_exist(self.cache_path):
            self.load(self.cache_path)
        else:
            self.compute()
            self.dump(self.cache_path)
