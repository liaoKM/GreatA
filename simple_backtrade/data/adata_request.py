import adata
import pandas
from multiprocessing import Pool
import os
import numpy as np
import datetime

def update(start_year:int,end_year:int,cache_path="./stock_data",parallel=True,clean=False):
    
    if clean==True or os.path.exists(os.path.join(cache_path,'all_finance_data.csv'))==False:
        request_finance_data()
    
    if clean==True or os.path.exists(os.path.join(cache_path,'all_XRXD_data.csv'))==False:
        request_xrxd_data()
    
    request_list=[]
    if clean==True:
        request_list=list(range(start_year,end_year+1))
    else:
        for year in range(start_year,end_year+1):
            if os.path.exists(os.path.join(cache_path,'market_data_{0}.csv'.format(year)))==False:
                request_list.append(year)
    if len(request_list)>0:
        if parallel:
            with Pool(len(request_list)) as p:
                p.map(request_market_data,request_list)
        else:
            for year in request_list:
                request_market_data(year)
    return

def request_finance_data(cache_path="./stock_data"):
    all_stocks=adata.stock.info.all_code()
    all_stocks=all_stocks.set_index('stock_code')
    finance_data_list:list[pandas.DataFrame]=[]
    for stock_code in all_stocks.index:
        finance_data=adata.stock.finance.get_core_index(stock_code)
        #忽略上市前财报（缅A全是诈骗 FUCK）
        if finance_data.empty == False:
            if all_stocks.loc[stock_code].list_date.__class__ == datetime.date:
                finance_data=finance_data[finance_data['report_date']>all_stocks.loc[stock_code].list_date.strftime("%Y-%m-%d")]
            finance_data_list.append(finance_data)

    pandas.concat(finance_data_list,ignore_index=True).to_csv(os.path.join(cache_path,'all_finance_data.csv'),index=False)        
    return

def request_market_data(year:int,cache_path="./stock_data"):
    all_stocks=adata.stock.info.all_code()
    all_stocks=all_stocks.set_index('stock_code')
    market_data_list=[]
    for stock_code in all_stocks.index:
        market_data=adata.stock.market.get_market(stock_code,start_date=str(year)+'-01-01',end_date=str(year+1)+'-12-31',k_type=1,adjust_type=0)
        if market_data.empty == False:
            if all_stocks.loc[stock_code].list_date.__class__ == datetime.date:
                market_data=market_data[market_data['trade_date']>all_stocks.loc[stock_code].list_date.strftime("%Y-%m-%d")]
            market_data_list.append(market_data)
    pandas.concat(market_data_list).to_csv(os.path.join(cache_path,'market_data_{0}.csv'.format(year)),index=False)    
    return

def request_xrxd_data(cache_path="./stock_data"):
    all_stocks=adata.stock.info.all_code()
    xrxd_data:list[pandas.DataFrame]=[]
    for stock in all_stocks.itertuples():
        try:
            data=adata.stock.market.get_dividend(stock.stock_code)
            xrxd_data.append(data)
        except:
            pass
    pandas.concat(xrxd_data).to_csv(os.path.join(cache_path,'all_XRXD_data.csv'),index=False)  
    return