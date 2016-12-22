import xml.etree.ElementTree as ET
from sim_engine.module_factory import ModuleFactory

class Engine(object):
    def __init__(self):
        self.module_factory = ModuleFactory()

    def parse_modules(self):
        tree = ET.parse('./config.xml')
        # Root node
        root = tree.getroot()

