# Seems that handler is not needed. For in C++ it is used to read .so files.
class HandlerBase(object):
    def __init__(self):
        # in the form of {module_id: module}
        self.module_list = {}

    def add_module(self, module_id, module):
        self.module_list[module_id] = module

    def get_module_list(self):
        return self.module_list
