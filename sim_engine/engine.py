import xml.etree.ElementTree as ET
from sim_engine.module_factory import ModuleFactory
from pipeline.module import (
    DataProvider,
    DataPortalModule,
    DailyLoopDataPortalModule
)
from sim_engine.context import Context
from sim_engine.run_case import RunCase
import re
import collections

# Map between variable name in Engine and module tag name in config.xml
# Tag name in config.xml : Variable name in Engine
# module_type_name_map = {
#     'Environment': 'Environment',
#     'Universe': 'Universe',
#     'Data': 'Data',
#     'Alpha': 'Alpha',
#     'Operation': 'Operation',
#     'Performance': 'Performance'
# }

class Engine(object):
    def __init__(self, config_path):
        self.config_path = config_path

        self.constants = {}
        self.paths = {}
        self.modules = {}
        self.xml_structure = {}

        self.module_factory = ModuleFactory()

        self.globals = Context()

        # In the format of {'mid1': ['data1', 'data2'], ...}
        self.data_source = {}
        # In the format of {'mid1': ['dependency1', 'dependency2'], ...}
        self.data_dependency = {}
        # In the format of [module1, module2, module3]
        self.daily_data_sequence = []
        # In the format of {'mid1': module_instance, ...}
        self.data_portal_modules = {}
        self.daily_data_portal_modules = {}

        # In the format of [data1, data2, ...]
        self.daily_portal_all_data = []

        self.real_dependency = set()
        self.case_list = []

    def parse_config(self):
        tree = ET.parse(self.config_path)
        # Root node
        root = tree.getroot()
        self.xml_structure = self._parse_node(root)

        engine._parse_path()
        engine._parse_constant()
        engine._parse_modules()

    def load_environment(self):
        """
        Load di_list and ii_list into self.globals
        :return:
        """
        environment_module = self.xml_structure['Environment']
        environment = self.module_factory.create_module(mid=environment_module['moduleId'],
                                                        context=self.globals,
                                                        params=environment_module)
        self.globals.di_list = environment.fetch_single_data('di_list')
        self.globals.ii_list = environment.fetch_single_data('ii_list')
        self.globals.start_di = self.globals.date_idx(self.globals.start_date)
        self.globals.end_di = self.globals.date_idx(self.globals.end_date)
        self.globals.set_shape(len(self.globals.ii_list))

    @staticmethod
    def _node_is_collection(node):
        """
        Return True if node is a list while False if node is a dictionary
        :param node:
        :return:
        """
        children = node.getchildren()
        if children is None or len(children) == 0:
            return False

        childnode_compare = children[0]
        for child in children:
            if childnode_compare.tag != child.tag:
                return False
        return True

    def _parse_node(self, node):
        if self._node_is_collection(node):
            structure = []
            for child in node:
                structure.append(self._parse_node(child))
        else:
            structure = node.attrib
            for child in node:
                structure[child.tag] = self._parse_node(child)

        return structure

    def _adjust_path(self, params):
        """
        Replace all the {PATH} in params with variable settings in <Paths></Paths>
        :param params: List containing all the parameters of a module
        :return:
        """
        for key in params.keys():
            # Pattern: {...}
            while re.compile(r'\{.*\}').match(params[key]):
                index_begin = params[key].index('{')
                index_end = params[key].index('}')
                mapped_str = params[key][index_begin+1:index_end]
                if mapped_str not in self.paths.keys():
                    raise Exception('Path {path} can not be found in config.xml'.format(
                        path=mapped_str
                    ))
                params[key] = params[key].replace('{' + mapped_str + '}', self.paths[mapped_str])

    def _parse_modules(self):
        self.modules = self.xml_structure['Modules']
        for module in self.modules:
            self._adjust_path(module)
            self.module_factory.register_module(mid=module['id'], params=module)

    def _parse_path(self):
        self.paths = self.xml_structure['Paths']

    def _parse_constant(self):
        self.constants = self.xml_structure['Constants']
        for key in self.constants.keys():
            self.globals.register_constant(key, self.constants[key])
        self.globals.start_date = self.globals.fetch_constant('startDate')
        self.globals.end_date = self.globals.fetch_constant('endDate')

    def analyze_dependency(self, xml_structure):
        """
        Analyze dependencies between data portal modules and create instance of DataPortalModules
        :param xml_structure:
        :return:
        """
        modules = xml_structure['Modules']
        for module in modules:
            mid = module['id']
            module = self.module_factory.create_module(mid=mid, context=self.globals, params=None)
            self.data_dependency[mid] = module.dependency
            if isinstance(module, DataProvider):
                self.data_source[mid] = module.data.keys()
                if isinstance(module, DataPortalModule):
                    self.data_portal_modules[mid] = module
                elif isinstance(module, DailyLoopDataPortalModule):
                    self.daily_data_portal_modules[mid] = module
                else:
                    pass
                    # raise Exception('Unrecognized data module {id}'.format(id=mid))

        print(self.data_source)
        print(self.data_dependency)

    def analyze_daily_data_sequence(self):
        _modules = self.daily_data_portal_modules.copy()

        _source = {}
        _dependency = {}
        for mid in _modules.keys():
            for data_name in _modules[mid].data.keys():
                self.daily_portal_all_data.append(data_name)
            _source[mid] = _modules[mid].data.keys()

        for mid in _modules.keys():
            _dependency[mid] = []
            for data_name in _modules[mid].dependency:
                if data_name in self.daily_portal_all_data:
                    _dependency[mid].append(data_name)

        while len(_dependency) != 0:
            _to_remove_data = []
            _to_remove_mid = []
            for mid in _dependency.keys():
                if len(_dependency[mid]) == 0:
                    self.daily_data_sequence.append(mid)
                    for data_name in _source[mid]:
                        _to_remove_data.append(data_name)
                    _to_remove_mid.append(mid)

            for mid in _to_remove_mid:
                del _dependency[mid]
            for mid in _dependency.keys():
                for data_name in _dependency[mid]:
                    if data_name in _to_remove_data:
                        _dependency[mid].remove(data_name)

    def _find_module_for_data(self, data_name):
        for module_id in self.data_source.keys():
            if data_name in self.data_source[module_id]:
                return module_id
        raise Exception

    def load_data(self, name):
        # if name in self.daily_portal_all_data:
        #     module_id = self._find_module_for_data(name)
        #     dependency = self.data_dependency[module_id]

        if self.globals.has_data(name):
            return

        # Find module id for the name
        mid = None
        for module_id in self.data_source.keys():
            if name in self.data_source[module_id]:
                mid = module_id
                break
        if mid is None:
            raise Exception('Data name {name} not exist'.format(name=name))

        # If the data is provided by a DataPortalModule instance,
        # then we need to fetch the data directly or build the module.
        if mid in self.data_portal_modules.keys():
            module = self.data_portal_modules[mid]
            if module.cache_exist():
                self.globals.register_data(name, module.fetch_single_data(name))
                self.globals.mark_loaded_data(name)
            else:
                dependency = self.data_dependency[mid]
                for data_name in dependency:
                    self.load_data(data_name)
                module.initialize()
                module.build()
                for name in module.data.keys():
                    self.globals.mark_loaded_data(name)

        # If the data is provided by a DailyDataPortalModule instance,
        # then we don't need to compute the data right now.
        # Here build() is only a preparation logic for calculation
        elif mid in self.daily_data_portal_modules.keys():
            module = self.daily_data_portal_modules[mid]
            dependency = self.data_dependency[mid]
            for data_name in dependency:
                self.load_data(data_name)
            module.initialize()
            module.build()

    @staticmethod
    def _set_add_list(_set, _list):
        for item in _list:
            _set.add(item)

    def generate_run_case(self, xml_structure):
        sim = xml_structure['Sim']

        for case_structure in sim:
            case_id = case_structure['id']
            case = RunCase(case_id=case_id)

            context = self.globals

            universe_structure = case_structure['Universe']
            universe_id = universe_structure['moduleId']
            self.real_dependency.add(universe_id)
            context.is_valid = self.globals.data_container[universe_id]

            alpha_structure = case_structure['Alpha']
            alpha_module = self.module_factory.create_module(mid=alpha_structure['moduleId'],
                                                             context=context,
                                                             params=alpha_structure)
            self._set_add_list(self.real_dependency, alpha_module.dependency)
            case.set_alpha_module(alpha_module)

            operations_structure = case_structure['Operations']
            for operation_info in operations_structure:
                operation_module = self.module_factory.create_module(mid=operation_info['moduleId'],
                                                                     context=context,
                                                                     params=operation_info)
                self._set_add_list(self.real_dependency, operation_module.dependency)
                case.add_operation_module(operation_module)

            performance_structure = case_structure['Performance'].copy()
            performance_structure['alpha_id'] = case_id
            performance_module = self.module_factory.create_module(mid=performance_structure['moduleId'],
                                                                   context=context,
                                                                   params=performance_structure)
            self._set_add_list(self.real_dependency, performance_module.dependency)
            case.set_performance_module(performance_module)

            self.case_list.append(case)

    def sim(self):
        print(self.xml_structure)
        sim_config = self.xml_structure['Sim']

        self.load_environment()
        print(sim_config)
        self.analyze_dependency(self.xml_structure)
        self.analyze_daily_data_sequence()
        self.generate_run_case(self.xml_structure)

        for data in self.real_dependency:
            self.load_data(data)

        for mid in self.daily_data_sequence:
            self.daily_data_portal_modules[mid].initialize()
        for case in self.case_list:
            case.initialize()

        start_di = self.globals.start_di
        end_di = self.globals.end_di

        for di in range(start_di, end_di+1):
            for mid in self.daily_data_sequence:
                self.daily_data_portal_modules[mid].start_day(di)
            for case in self.case_list:
                case.start_day(di)

            for mid in self.daily_data_sequence:
                self.daily_data_portal_modules[mid].end_day(di)
            for case in self.case_list:
                case.end_day(di)


if __name__ == '__main__':
    engine = Engine('F:\\zalpha\\zalpha\\config.xml')
    engine.parse_config()
    engine.sim()
