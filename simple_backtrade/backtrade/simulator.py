from ..data.data_manager import DataManager
from ..data.xt_data_downloader import XtDataDownloader
from ..account import SimpleAccount
from .. import strategy
from ..log import TradeLogger
    
import pandas
import re
from datetime import datetime, timedelta

class LocalSimulator:
    def __init__(self,_start_time:str,_end_time:str,_strategy:strategy.BaseStrategy,account:SimpleAccount=None):

        downloader=XtDataDownloader()
        self.start_time=datetime.strptime(_start_time, "%Y-%m-%d")
        self.end_time=datetime.strptime(_end_time, "%Y-%m-%d")
        self.data_manager=DataManager(downloader,self.start_time-timedelta(30),self.end_time)
        self.marketday_list=self.data_manager.market_data.loc[self.start_time:self.end_time,:].index.get_level_values('date').unique()
        self.stocks=self.data_manager.market_data.index.get_level_values('stock_code').unique()

        #account
        if account is not None:
            self.account=account
        else:
            self.account=SimpleAccount(1_000_000)
        #strategy
        if _strategy is not None:
            self.strategy=_strategy
        else:
            self.strategy=strategy.MyStrategy(self.data_manager)
        #logger
        self.logger=TradeLogger()
        return
    
    def __XRXD(self,account:SimpleAccount,prev_date:datetime,date:datetime):
        '''除权除息'''
        divid_data=self.data_manager.divid_data.loc[prev_date+timedelta(1):date]
        if divid_data.empty:
            return
        #self.strategy.handel_devid(date)#todo
        intersect_index=account.stocks.index.intersection(divid_data.index.get_level_values('stock_code'))
        if intersect_index.empty:
            return
        divid_data=divid_data.swaplevel('date','stock_code')
        divid_data=divid_data.loc[intersect_index]
        for (stock_code,_),data_row in divid_data.iterrows():
            account.stocks.loc[stock_code,'num']*=(1+data_row.stockBonus+data_row.stockGift)
            account.money+=account.stocks.loc[stock_code,'num']*data_row.interest
            self.account.buyin_price.loc[stock_code,'price']/=(1+data_row.stockBonus+data_row.stockGift+data_row.interest)
        return
    
    def start(self):
        prev_date=self.start_time-timedelta(1)
        self.strategy.handle_report(prev_date,self.stocks)

        for date in self.marketday_list:
            #分红送股
            self.__XRXD(self.account,prev_date,date)

            #K线更新
            keep_stocks=self.strategy.handle_bar(date)
            self.daily_settlement(date,keep_stocks.index)

            #新财报
            new_report_stocks=self.data_manager.finance_data[(self.data_manager.finance_data.m_anntime>prev_date)&(self.data_manager.finance_data.m_anntime<=date)].index.get_level_values('stock_code').unique()
            self.strategy.handle_report(date,new_report_stocks)

            prev_date=date

        self.logger.prepare_analysis(self.data_manager.market_data.swaplevel('stock_code','date').loc['000300.SH'][self.start_time:self.end_time])
        analyze_result=self.logger.analyze()
        return analyze_result
    
    def daily_settlement(self,date:datetime,stock_list:pandas.Index):
        self.account.sell_all(self.data_manager,date,self.logger)
        self.account.estimate_asset(date,self.logger)
        self.account.buyin(self.data_manager,date,stock_list)

        return