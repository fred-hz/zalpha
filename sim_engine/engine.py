import xml.etree.ElementTree as ET
from sim_engine.module_factory import ModuleFactory
from sim_engine.context import Context
import re

class Engine(object):
    def __init__(self, config_path):
        self.module_factory = ModuleFactory()
        self.config_path = config_path
        self.xml_structure = {}

        self.module_factory = ModuleFactory()
        self.context = Context()
        self.constants = {}
        self.paths = {}

    def parse_config(self):
        tree = ET.parse(self.config_path)
        # Root node
        root = tree.getroot()
        self.xml_structure = self._parse_node(root)

    @staticmethod
    def _node_is_collection(node):
        if 'type' in node.attrib.keys() and node.attrib['type'] == 'collection':
            return True
        else:
            return False

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

    def parse_modules(self):
        module_list = self.xml_structure['Modules']
        for module_para in module_list:
            mid = module_para['id']
            # Justify full path name
            for key in module_para.keys():
                while re.compile('\$\{.*\}').match(module_para[key]):
                    begin = module_para[key].find('${')
                    end = module_para[key].find('}', begin)
                    # e.g. to_replace_str can be ${CACHE} while paired_key can be CACHE
                    to_replace_str = module_para[key][begin:end+1]
                    paired_key = module_para[key][begin+2:end]
                    if paired_key not in self.paths.keys():
                        raise Exception('Path {str} can not be found in config.xml'.format(
                            str=paired_key
                        ))
                    module_para[key] = module_para[key].replace(to_replace_str, self.paths[paired_key])

            self.module_factory.register_module(mid, module_para)

    def parse_path(self):
        self.paths = self.xml_structure['Paths']

    def parse_constant(self):
        self.constants = self.xml_structure['Constants']


if __name__ == '__main__':
    engine = Engine('/Users/Onlyrabbit/PycharmProjects/zalpha/config.xml')
    engine.parse_config()
    engine.parse_path()
    engine.parse_constant()
    engine.parse_modules()