from sim_engine.module_factory import ModuleFactory
import numpy as np
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

    def start_day(self, di):
        self.alpha_module.start_day(di)

    def end_day(self, di):
        for module in self.operation_modules:
            module.end_day(di)
        self.performance_module.end_day(di)

    def initialize(self):
        self.alpha_module.initialize()
        for module in self.operation_modules:
            module.initialize()
        self.performance_module.initialize()
