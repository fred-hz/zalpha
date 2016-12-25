from sim_engine.engine import Engine

class Sim(object):

    def __init__(self):
        self.engine = Engine()

    def run(self):
        self.engine.parse_config()
        self.engine.parse_environment()
        self.engine.parse_modules()

sim = Sim()
sim.run()