import xml.etree.ElementTree as ET
from sim_engine.module_factory import ModuleFactory


class Engine(object):
    def __init__(self, config_path):
        self.module_factory = ModuleFactory()
        self.config_path = config_path
        self.xml_structure = {}

        self.module_factory = ModuleFactory()

    def parse_config(self):
        tree = ET.parse(self.config_path)
        # Root node
        root = tree.getroot()
        self.xml_structure = self._parse_node(root)
        print(self.xml_structure)

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
            self.module_factory.register_module(mid, module_para)


if __name__ == '__main__':
    engine = Engine('/Users/Onlyrabbit/PycharmProjects/zalpha/config.xml')
    engine.parse_config()