import matplotlib.pyplot as plt
import simple_backtrade
from multiprocessing import Pool

def task(year):
    start_time='{}-01-01'.format(year)
    end_time='{}-12-31'.format(year)
    simulator=simple_backtrade.backtrade.LocalSimulator(start_time,end_time)
    simulator.start()


if __name__=='__main__':
    simple_backtrade.adata_request.update(2008,2016)
    param_list=[2017,2018,2019,2020,2021,2022,2023,2024]
    with Pool(len(param_list)) as p:
        p.map(task,param_list)
    # task(2019)