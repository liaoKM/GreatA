import pandas
import os
from datetime import datetime, timedelta
import adata

from . import adata_request
from .base_manager import BaseManager,FinanceReportType

class LocalDataManager(BaseManager):
    def __init__(self,source_path="./stock_data"):
        super(LocalDataManager,self).__init__()
        self.source_path="./stock_data"
        self.finance_data:pandas.DataFrame=None
        self.market_data:pandas.DataFrame=None
        self.xrxd_data:pandas.DataFrame=None
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
    
    def init_range(self,start_time,end_time,stocks=[]):
        super(LocalDataManager,self).init_range(start_time,end_time,stocks)
        self.data_range=(start_time,end_time)

        start_year=int(start_time[:4])
        end_year=int(end_time[:4])
        adata_request.update(start_year,end_year,self.source_path)

        entire_finance_data=self.__load_finance_data()
        entire_xrxd_data=self.__load_xrxd_data()
        market_data_list=[]
        for i in range(start_year,end_year+1):
            market_data_list.append(self.__load_market_data(i))
        market_data=pandas.concat(market_data_list)

        filtered_finance_data=entire_finance_data#[(entire_finance_data['notice_date']>=start_time)&(entire_finance_data['notice_date']<=end_time)]
        filtered_xrxd_data=entire_xrxd_data[(entire_xrxd_data['ex_dividend_date']>=start_time)&(entire_xrxd_data['ex_dividend_date']<=end_time)]
        filtered_market_data=market_data[(market_data['trade_date']>=start_time)&(market_data['trade_date']<=end_time)]
        if len(stocks)>0:
            filtered_finance_data=filtered_finance_data[filtered_finance_data['stock_code'].isin(stocks)]
            filtered_market_data=filtered_market_data[filtered_market_data['stock_code'].isin(stocks)]
        self.finance_data=filtered_finance_data.sort_values("notice_date").set_index('stock_code')
        self.market_data=filtered_market_data.sort_values("trade_date").set_index('stock_code')
        self.xrxd_data=filtered_xrxd_data.set_index('ex_dividend_date').sort_index()

        baseline_start_time=(datetime.strptime(start_time, "%Y-%m-%d")-timedelta(days=365*2)).strftime("%Y-%m-%d")#取400日均线
        self.baseline=adata.stock.market.get_market_index('000300',baseline_start_time).sort_values("trade_date")

        return
    
    def get_recent_baseline(self,date,count=1):
        self.baseline[self.baseline['trade_date']<=date].tail(count)
        return self.baseline[self.baseline['trade_date']<=date].tail(count)
    
    def get_baseline(self,start_time,end_time):
        dataframe=self.baseline[(self.baseline['trade_date']<=end_time)&(self.baseline['trade_date']>=start_time)]
        return dataframe
    
    def get_daily_market_data(self,date,stocks=[])->pandas.DataFrame:
        if len(stocks)>0:
            valid_index=[stock for stock in stocks if stock in self.market_data.index]
            market_data=self.market_data.loc[valid_index]
            data_frames=market_data[market_data["trade_date"]==date]
        else:
            data_frames=self.market_data[self.market_data["trade_date"]==date]
        return data_frames
    
    def get_stock_market_data(self,stock_code,start_time,end_time)->pandas.DataFrame:
        assert(start_time>=self.data_range[0] and end_time<=self.data_range[1])
        market_data=self.market_data.loc[stock_code]
        data_frames=market_data[(market_data["trade_date"]>=start_time)&(market_data["trade_date"]<=end_time)]
        return data_frames
    
    def get_recent_stock_market_data(self,stock_code,now_date,count=1)->pandas.DataFrame:
        market_data=self.market_data.loc[stock_code]
        data_frames=market_data[market_data["trade_date"]<=now_date]
        return data_frames.tail(count)
    
    def get_recent_finance_data(self,now_date,stock_code,count=1,report_type:FinanceReportType=FinanceReportType.ANY)->pandas.DataFrame:
        finance_data=self.finance_data.loc[stock_code]
        if report_type!=FinanceReportType.ANY:
            data_frame=finance_data[(finance_data["report_type"]==report_type.value)&(finance_data["notice_date"]<=now_date)]
        else:
            data_frame=finance_data[finance_data["notice_date"]<=now_date]
        data_frame=data_frame.sort_values(by='report_date')
        return data_frame.tail(count)
    
    def get_xrxd_data(self,start_time,end_time)->pandas.DataFrame:
        assert(start_time>=self.data_range[0] and end_time<=self.data_range[1])
        try:
            start_pos = self.xrxd_data.index.get_loc(start_time).start
            end_pos = self.xrxd_data.index.get_loc(end_time).stop+1
            data_frames=self.xrxd_data.iloc[start_pos:end_pos]
        except:
            data_frames=self.xrxd_data.iloc[0:0]
        return data_frames
    
    def get_noticed_finance_report(self,start_time,end_time,stocks=[])->pandas.DataFrame:
        assert(start_time>=self.data_range[0] and end_time<=self.data_range[1])
        if len(stocks)>0:
            finance_data=self.finance_data.loc[stocks]
            data_frames=finance_data[(finance_data["notice_date"]<=end_time)&(finance_data["notice_date"]>=start_time)]
        else:
            data_frames=self.finance_data[(self.finance_data["notice_date"]<=end_time)&(self.finance_data["notice_date"]>=start_time)]

        try:
            data_frame=data_frame.sort_values(by='report_date')
        except:
            data_frame=None

        return data_frames
    
    def get_marketday_list(self,start_time,end_time)->list[str]:
        marketday_list=list(self.market_data[(self.market_data["trade_date"]>=start_time)&(self.market_data["trade_date"]<=end_time)]["trade_date"].unique())
        return marketday_list
    
    def get_all_stockcode(self)->list[str]:
        stocks=list(self.market_data.index.unique())
        return stocks