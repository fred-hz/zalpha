from sim_engine.engine import Engine
import sys

class Sim(object):

    def __init__(self, config_path='/Users/Onlyrabbit/PycharmProjects/zalpha/config.xml'):
        self.engine = Engine(config_path=config_path)

    def run(self):
        self.engine.parse_config()

if __name__ == '__main__':
    if len(sys.argv) > 1:
        sim = Sim(sys.argv[1])
    else:
        sim = Sim()
    sim.run()