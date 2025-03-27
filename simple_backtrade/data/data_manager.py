import pandas
from pandas import IndexSlice as Idx
import os
from datetime import datetime, timedelta
import adata

from .base_manager import BaseManager,FinanceReportType

class LocalDataManager(BaseManager):
    def __init__(self,source_path="./stock_data"):
        super(LocalDataManager,self).__init__()
        self.source_path="./stock_data"
        self.finance_data:pandas.DataFrame=None
        self.market_data:pandas.DataFrame=None
        self.xrxd_data:pandas.DataFrame=None
        self.data_start_time=None
        self.data_end_time=None
        return
    
    def __load_finance_data(self)->pandas.DataFrame:
        data_types={'stock_code':str}
        finance_data=pandas.read_csv(os.path.join(self.source_path,'all_finance_data.csv'),dtype=data_types)
        return finance_data

    def __load_market_data(self,year:int)->pandas.DataFrame:
        data_types={'stock_code':str}
        market_data=pandas.read_csv(os.path.join(self.source_path,'market_data_{0}.csv'.format(year)),dtype=data_types)
        return market_data
    
    def __load_xrxd_data(self)->pandas.DataFrame:
        data_types={'stock_code':str}
        xrxd_data=pandas.read_csv(os.path.join(self.source_path,'all_XRXD_data.csv'),dtype=data_types)
        return xrxd_data
    
    def init_range(self,start_time:datetime,end_time:datetime,stocks=[]):
        super(LocalDataManager,self).init_range(start_time,end_time,stocks)
        self.data_range=(start_time,end_time)

        entire_finance_data=self.__load_finance_data()
        entire_xrxd_data=self.__load_xrxd_data()
        market_data_list=[]
        for i in range(start_time.year,end_time.year+1):
            market_data_list.append(self.__load_market_data(i))
        market_data=pandas.concat(market_data_list)
        entire_finance_data['notice_date']=pandas.to_datetime(entire_finance_data['notice_date'], format='%Y-%m-%d')
        entire_xrxd_data['ex_dividend_date']=pandas.to_datetime(entire_xrxd_data['ex_dividend_date'], format='%Y-%m-%d')
        entire_xrxd_data['report_date']=pandas.to_datetime(entire_xrxd_data['report_date'], format='%Y-%m-%d')
        market_data['trade_date']=pandas.to_datetime(market_data['trade_date'], format='%Y-%m-%d')

        filtered_finance_data=entire_finance_data#[(entire_finance_data['notice_date']>=start_time)&(entire_finance_data['notice_date']<=end_time)]
        filtered_xrxd_data=entire_xrxd_data[(entire_xrxd_data['ex_dividend_date']>=start_time)&(entire_xrxd_data['ex_dividend_date']<=end_time)]
        filtered_market_data=market_data[(market_data['trade_date']>=start_time)&(market_data['trade_date']<=end_time)]
        if len(stocks)>0:
            filtered_finance_data=filtered_finance_data[filtered_finance_data['stock_code'].isin(stocks)]
            filtered_market_data=filtered_market_data[filtered_market_data['stock_code'].isin(stocks)]
            filtered_xrxd_data=filtered_xrxd_data[filtered_xrxd_data['stock_code'].isin(stocks)]

        filtered_finance_data=filtered_finance_data.dropna(subset='notice_date')
        self.finance_data=filtered_finance_data.set_index(['notice_date','stock_code']).sort_index(level=['notice_date', 'stock_code'])
        filtered_market_data=filtered_market_data.dropna(subset='trade_date')
        self.market_data=filtered_market_data.set_index(['trade_date','stock_code']).sort_index(level=['trade_date', 'stock_code'])
        filtered_xrxd_data=filtered_xrxd_data.dropna(subset='ex_dividend_date')
        self.xrxd_data=filtered_xrxd_data.set_index('ex_dividend_date').sort_index()

        baseline_start_time=(start_time-timedelta(days=365*2)).strftime("%Y-%m-%d")#取400日均线
        self.baseline=adata.stock.market.get_market_index('000300',baseline_start_time)
        self.baseline['trade_date']=pandas.to_datetime(self.baseline['trade_date'], format='%Y-%m-%d')
        self.baseline=self.baseline.dropna(subset='trade_date').set_index('trade_date').sort_index()

        return
    
    def get_recent_baseline(self,date:datetime,count=1):
        return self.baseline.loc[:date].tail(count)
    
    def get_baseline(self,start_time,end_time):
        return self.baseline.loc[start_time:end_time]
    
    def get_daily_market_data(self,date,stocks=[])->pandas.DataFrame:
        valid_stocks=self.market_data.index.get_level_values('stock_code').unique().intersection(stocks)
        market_data=self.market_data.loc[Idx[date,valid_stocks],:]
        return market_data.droplevel('trade_date')
    
    
    def get_recent_stock_market_data(self,stock_code:str,now_date:str,count=1)->pandas.DataFrame:
        market_data=self.market_data.loc[Idx[:now_date,stock_code],:]
        return market_data.groupby(level='stock_code').tail(count)
    
    def get_recent_finance_data(self,now_date:datetime,stock_code:str,count=1,report_type:FinanceReportType=FinanceReportType.ANY)->pandas.DataFrame:
        valid_code=self.finance_data.index.get_level_values('stock_code').unique().intersection(stock_code)
        finance_data=self.finance_data.loc[Idx[:now_date,valid_code],:]
        if report_type!=FinanceReportType.ANY:
            finance_data=finance_data[finance_data['report_type']==report_type.value]
        recent_data=finance_data.sort_values(by='report_date').groupby(level='stock_code').tail(count)
        return recent_data
    
    def get_xrxd_data(self,start_time,end_time)->pandas.DataFrame:
        assert(start_time>=self.data_range[0] and end_time<=self.data_range[1])
        #end_time=end_time+timedelta(days=1)#[start_time,endtime) -> [start_time,end_time]
        return self.xrxd_data.loc[start_time:end_time]
    
    def get_noticed_finance_report(self,start_time:datetime,end_time:datetime,stocks=[])->pandas.DataFrame:
        assert(start_time>=self.data_range[0] and end_time<=self.data_range[1])
        #end_time=end_time+timedelta(days=1)#[start_time,endtime) -> [start_time,end_time]
        valid_stocks=self.finance_data.index.get_level_values('stock_code').intersection(stocks)
        try:
            finance_data=self.finance_data.loc[Idx[start_time:end_time,valid_stocks],:]
        except:
            finance_data=self.finance_data.iloc[0:0]
        return finance_data
    
    def get_marketday_list(self,start_time:datetime,end_time:datetime)->list[str]:
        return self.market_data[start_time:end_time].index.get_level_values('trade_date').unique()
    
    def get_all_stockcode(self)->list[str]:
        return self.market_data.index.get_level_values('stock_code').unique()