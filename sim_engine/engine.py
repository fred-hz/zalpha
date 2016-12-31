import xml.etree.ElementTree as ET
from sim_engine.module_factory import ModuleFactory
from sim_engine.context import Context
import re

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
        self.module_factory = ModuleFactory()
        self.config_path = config_path
        self.xml_structure = {}

        self.context = Context()
        self.constants = {}
        self.paths = {}

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
        module_structure = self.xml_structure['Modules']

        for module_type_name in module_type_name_map.keys():
            module_list_by_type = module_structure[module_type_name]
            for module in module_list_by_type:
                self._adjust_path(module)
                self.module_factory.register_module(mid=module['id'], paras=module)

    def _parse_path(self):
        self.paths = self.xml_structure['Paths']

    def _parse_constant(self):
        self.constants = self.xml_structure['Constants']

    def init_alpha_test(self):
        alpha_test_config = self.xml_structure['AlphaTest']
        print(alpha_test_config)
        for module in alpha_test_config:
            pass


if __name__ == '__main__':
    engine = Engine('/Users/Onlyrabbit/PycharmProjects/zalpha/config.xml')
    engine.parse_config()
    print(engine.xml_structure)
    #engine.init_alpha_test()
