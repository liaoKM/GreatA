import pandas
import numpy as np
import os
import matplotlib.pyplot as plt
from multiprocessing import Pool
import RawData
from Strategy import BaseStrategy
from Strategy import ActionLogger

def process_task(date_range):
    finance_data=RawData.get_finance_data()
    date_range_str=[date._date_repr for date in date_range]
    market_data=RawData.get_market_daily_data(date_range[0].year)
    market_data=market_data[market_data['trade_date'].isin(date_range_str)]
    strategy=BaseStrategy(finance_data,market_data)
    pre_filter_list=strategy.pre_filter(date_range[0].year)
    strategy.simulate(pre_filter_list,date_range)
    return strategy.action_logger.actions

def gen_date_range(year:int):
    range_list=[]
    for month in range(1,12):
        date_range = pandas.date_range('{0}-{1:0>2d}-01'.format(year,month), '{0}-{1:0>2d}-01'.format(year,month+1))
        range_list.append(date_range)
    date_range = pandas.date_range('{0}-{1:0>2d}-01'.format(year,12), '{0}-{1:0>2d}-01'.format(year+1,1))
    range_list.append(date_range)
    return range_list

if __name__=='__main__':
    
    profit_dataframe_list=[]
    start_year=2017
    end_year=2021

    # WarmUp: request data and save to csv
    # finance_data=RawData.get_finance_data()
    # year_list=list(range(start_year,end_year+1))
    # with Pool(len(year_list)) as p:
    #     p.map(RawData.get_market_daily_data,year_list)
    
    for year in range(start_year,end_year+1):
        range_list = gen_date_range(year)

        # single thread
        # action_list=[]
        # for range in range_list:
        #     actions=process_task(range)
        #     action_list.append(actions)

        #parallel
        with Pool(12) as p:
            action_list=p.map(process_task,range_list)
        actions=pandas.concat(action_list)
        actions.to_csv('./result/actions-{0}.csv'.format(year),index=False)
        logger=ActionLogger()
        logger.actions=actions
        avg_info=logger.get_daily_avg_info()
        avg_info['cum_profit']=avg_info['profit'].cumprod()
        avg_info[['cum_profit','pe']].plot(secondary_y='pe')
        plt.savefig('result/profit_{0}.png'.format(year))
        plt.clf()

    pass