import adata
import pandas
import os
def request_finance_data(output="./stock_data"):
    all_stocks=adata.stock.info.all_code()
    finance_data:list[pandas.DataFrame]=[]
    for stock in all_stocks.itertuples():
        finance_data.append(adata.stock.finance.get_core_index(stock.stock_code))
    pandas.concat(finance_data).to_csv(os.path.join(output,'all_finance_data.csv'),index=False)        
    return

def request_daily_data(year:int,output="./stock_data"):
    all_stocks=adata.stock.info.all_code()
    market_data_list=[]
    for stock in all_stocks.itertuples():
        price=adata.stock.market.get_market(stock.stock_code,start_date=str(year)+'-01-01',end_date=str(year+1)+'-12-31',k_type=1,adjust_type=0)#不复权用于计算市值
        market_data=adata.stock.market.get_market(stock.stock_code,start_date=str(year)+'-01-01',end_date=str(year+1)+'-12-31',k_type=1,adjust_type=2)
        if price.empty:
            continue
        market_data['stock_price']=price['close']
        market_data_list.append(market_data)
        
        #market_data.to_csv(os.path.join(output,str(year),'{0}.csv'.format(stock.stock_code)),index=False)
    pandas.concat(market_data_list).to_csv(os.path.join(output,'market_data_{0}.csv'.format(year)),index=False)    
    return

def get_finance_data(cache_path:str="./stock_data"):
    if os.path.exists(os.path.join(cache_path,'all_finance_data.csv'))==False:
        request_finance_data(cache_path)
    data_types={'stock_code':str}
    all_finance_data=pandas.read_csv(os.path.join(cache_path,'all_finance_data.csv'),dtype=data_types)
    return all_finance_data

def get_market_daily_data(year:int,cache_path:str="./stock_data"):
    if os.path.exists(os.path.join(cache_path,'market_data_{0}.csv'.format(year)))==False:
        request_daily_data(year,cache_path)
    data_types={'stock_code':str}
    market_data=pandas.read_csv(os.path.join(cache_path,'market_data_{0}.csv'.format(year)),dtype=data_types)
    return market_data