import xml.etree.ElementTree as ET

class ModuleFactory(object):
    def __init__(self):
        # Save modules. In the format of {mid: {variables values...}}
        self.modules = {}
        self.required_columns = ['class']

    def register_module(self, mid, params):
        """
        Register modules into module factory. Fetch one module object later when needed.
        :param mid: Module id in config.xml
        :param params: A dict containing all the parameters
        :return:
        """
        # check all the required parameters in kwargs
        qualified = True
        for column in self.required_columns:
            if column not in params.keys():
                qualified = False
                break

        if not qualified:
            raise Exception('not all required columns are met in module {mid}'.format(
                mid=mid
            ))

        self.modules[mid] = params

    def create_module(self, mid, context, params):
        """
        Create a module object according to the class name of module mid.
        :param mid: Module id in config.xml
        :param params: New paras from config.xml asides from already registered parameters
        :param context:
        :return:
        """
        class_name = self.modules[mid]['class']
        names = class_name.split('.')
        # Module path in python environment
        python_module_path = '.'.join(names[0:-1])
        # Import python module
        python_module = __import__(python_module_path)
        # Fetch target class
        module_file = getattr(python_module, names[-2])
        module_class = getattr(module_file, names[-1])

        new_params = self.modules[mid].copy()
        if params is not None and len(params) != 0:
            new_params.update(params)

        return module_class(params=new_params, context=context)

    def __str__(self):
        return str(self.modules)


if __name__ == '__main__':
    module_factory = ModuleFactory()
    # module_factory.create_module('alpha.alpha_base.AlphaBase')
