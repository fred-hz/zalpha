import xml.etree.ElementTree as ET
from sim_engine.module_factory import ModuleFactory
from sim_engine.context import Context
import re
import collections

# Map between variable name in Engine and module tag name in config.xml
# Tag name in config.xml : Variable name in Engine
module_type_name_map = {
    'Environment': 'Environment',
    'Universe': 'Universe',
    'Data': 'Data',
    'Alpha': 'Alpha',
    'Operation': 'Operation',
    'Performance': 'Performance'
}

class Engine(object):
    def __init__(self, config_path):
        self.config_path = config_path

        self.context = Context()
        self.constants = {}
        self.paths = {}
        self.xml_structure = {}

        self.module_factory = ModuleFactory(context=self.context)

        self.environment_module = None
        self.performance_module = None
        self.alpha_modules = {}
        self.universe_modules = {}
        self.operation_modules = collections.OrderedDict()

    def parse_config(self):
        tree = ET.parse(self.config_path)
        # Root node
        root = tree.getroot()
        self.xml_structure = self._parse_node(root)

        engine._parse_path()
        engine._parse_constant()
        engine._parse_modules()

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

        for module_type_name in module_type_name_map.keys():
            module_list_by_type = self.modules[module_type_name]
            for module in module_list_by_type:
                self._adjust_path(module)
                self.module_factory.register_module(mid=module['id'], params=module)

    def _parse_path(self):
        self.paths = self.xml_structure['Paths']

    def _parse_constant(self):
        self.constants = self.xml_structure['Constants']
        for key in self.constants.keys():
            self.context.register_constant(key, self.constants[key])

    def preload_modules(self, sim_config):
        """
        Create instance of declared modules in AlphaTest and all Data modules in config.
        However, modules are not ran here.
        :param sim_config:
        :return:
        """
        config = sim_config.copy()
        environment = config['Environment']
        performance = config['Performance']
        alpha_universe_list = config['Alphas']
        operations_list = config['Operations']

        environment_id = environment['moduleId']
        environment.pop('moduleId')
        self.environment_module = self.module_factory.create_module(mid=environment_id,
                                                                    params=environment)

        performance_id = performance['moduleId']
        performance.pop('moduleId')
        self.performance_module = self.module_factory.create_module(mid=performance_id,
                                                                    params=performance)

        for instance in alpha_universe_list:
            alpha_id = instance['alphaId']
            universe_id = instance['universeId']
            del instance['alphaId']
            del instance['universeId']
            alpha_module = self.module_factory.create_module(mid=alpha_id,
                                                             params=instance)
            universe_module = self.module_factory.create_module(mid=universe_id,
                                                                params=None)
            self.alpha_modules[alpha_id] = alpha_module
            self.universe_modules[universe_id] = universe_module

        for instance in operations_list:
            operation_id = instance['moduleId']
            del instance['moduleId']
            operation_module = self.module_factory.create_module(mid=operation_id,
                                                                 params=instance)
            self.operation_modules[operation_id] = operation_module
        print(self.alpha_modules)

    def construct_dependency_tree(self):
        pass

    def sim(self):
        sim_config = self.xml_structure['AlphaTest']
        id = sim_config['id']
        delay = sim_config['delay']
        method = sim_config['method']
        ndays = sim_config['ndays']
        startDate = sim_config['startDate']
        endDate = sim_config['endDate']

        self.context.start_date = startDate
        self.context.end_date = endDate

        self.preload_modules(sim_config)

if __name__ == '__main__':
    engine = Engine('/Users/Onlyrabbit/PycharmProjects/zalpha/config.xml')
    engine.parse_config()
    engine.sim()
