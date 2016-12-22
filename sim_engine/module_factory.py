import xml.etree.ElementTree as ET

class ModuleFactory(object):
    def __init__(self):
        # Save modules. In the format of {mid: {variables values...}}
        self.modules = {}

        self.required_columns = ['class']

    def register_module(self, mid, *args, **kwargs):
        qualified = True
        for column in self.required_columns:
            if column not in kwargs.keys():
                qualified = False
                break

        if not qualified:
            raise Exception('not all required columns are meet in module {mid}'.format(
                mid=mid
            ))

        self.modules[mid] = kwargs

