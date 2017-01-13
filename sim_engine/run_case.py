from sim_engine.module_factory import ModuleFactory
class RunCase(object):
    def __init__(self, case_id):
        self.case_id = case_id
        self.alpha_module = None
        self.operation_modules = []
        self.performance_module = None

    def set_alpha_module(self, module):
        self.alpha_module = module

    def set_performance_module(self, module):
        self.performance_module = module

    def add_operation_module(self, module):
        self.operation_modules.append(module)

    def start_day(self):
        self.alpha_module.start_day()

    def end_day(self):
        for module in self.operation_modules:
            module.end_day()
        self.performance_module.end_day()
