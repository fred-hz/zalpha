from data_manager.dm_basedata import DataManagerBaseData


class Sim(object):

    def __init__(self):
        pass

    def run(self):
        data_path = ''
        mid = ''
        dmgr_base_data = DataManagerBaseData(mid, data_path)
        dmgr_base_data.compute()

sim = Sim()
sim.run()