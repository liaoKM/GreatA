import matplotlib.pyplot as plt
import simple_backtrade
from multiprocessing import Pool
import numpy as np
import pandas
from simple_backtrade.log import TradeLogger

def task(year):
    start_time='{}-01-01'.format(year)
    end_time='{}-12-31'.format(year)
    simulator=simple_backtrade.backtrade.LocalSimulator(start_time,end_time)
    analyze_result=simulator.start()
    return analyze_result


if __name__=='__main__':
    #simple_backtrade.adata_request.update(2007,2025)
    param_list=list(np.arange(2008,2026))
    with Pool(len(param_list)) as p:
        analyze_results=p.map(task,param_list)
    #task(2008)
    
    daily_returns=pandas.concat([analyze_result['daily_returns'] for analyze_result in analyze_results])
    benchmark=pandas.concat([analyze_result['benchmark'] for analyze_result in analyze_results])
    merge_logger=TradeLogger()
    merge_logger.daily_returns=daily_returns
    merge_logger.benchmark=benchmark
    merge_logger.analyze()