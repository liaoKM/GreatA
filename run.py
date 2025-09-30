import matplotlib.pyplot as plt
import simple_backtrade
from multiprocessing import Pool
import numpy as np
import pandas
from simple_backtrade.log import TradeLogger
import datetime

def task(year):
    start_time='{}-01-01'.format(year)
    end_time='{}-12-31'.format(year)
    simulator=simple_backtrade.backtrade.LocalSimulator(start_time,end_time,None)
    analyze_result=simulator.start()
    return analyze_result


if __name__=='__main__':

    analyze_results=[]
    for i in range(2014,2026):
        analyze_results.append(task(i))
    # param_list=list(np.arange(2012,2024))
    # with Pool(len(param_list)) as p:
    #     analyze_results=p.map(task,param_list)

    daily_returns=pandas.concat([analyze_result['daily_returns'] for analyze_result in analyze_results])
    benchmark=pandas.concat([analyze_result['benchmark'] for analyze_result in analyze_results])
    merge_logger=TradeLogger()
    merge_logger.daily_returns=daily_returns
    merge_logger.benchmark=benchmark
    merge_logger.analyze()