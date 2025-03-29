from ..data.data_manager import LocalDataManager
from ..account import SimpleAccount
from .. import strategy
from ..log import TradeLogger
    
import pandas
import re
from datetime import datetime, timedelta

class LocalSimulator:
    def __init__(self,_start_time:str,_end_time:str,account:SimpleAccount=None):
        self.data_manager=LocalDataManager()
        self.start_time=datetime.strptime(_start_time, "%Y-%m-%d")
        self.end_time=datetime.strptime(_end_time, "%Y-%m-%d")
        self.data_manager.init_range(self.start_time-timedelta(days=100),self.end_time)
        if account is not None:
            self.account=account
        else:
            self.account=SimpleAccount(1_000_000)
        
        #交易时间
        self.marketday_list=self.data_manager.get_marketday_list(self.start_time,self.end_time)
        self.strategy=strategy.BaseStrategy(self.account,self.data_manager,self.start_time)
        self.logger=TradeLogger()
        return
    
    def __XRXD(self,account:SimpleAccount,prev_date:datetime,date:datetime):
        '''除权除息'''
        start_time=prev_date+timedelta(days=1)
        xrxd_datas=self.data_manager.get_xrxd_data(start_time,date)
        for index,data in xrxd_datas.iterrows():
            if data.stock_code in account.stocks.index:
                xr_pattern='10股转赠(.*?)股'
                xd_pattern='10股派(.*?)元'
                right=re.findall(xr_pattern,data.dividend_plan)
                dividend=re.findall(xd_pattern,data.dividend_plan)
                assert len(right)<=1 and len(dividend)<=1
                if len(right)!=1 and len(dividend)!=1:
                    print("[Warning]:{0} xrxd data missing!",data.stock_code)
                takes_num=account.stocks.loc[data.stock_code,'num']
                if len(dividend)==1:
                    account.money+=int(takes_num/10)*float(dividend[0])
                if len(right)==1:
                    account.stocks.at[data.stock_code,'num']+=int(int(takes_num/10)*float(right[0]))
        return
    
    def __get_new_finance_report(self,prev_date:datetime,date:datetime)->pandas.DataFrame:
        start_time=prev_date+timedelta(days=1)
        finance_report_data=self.data_manager.get_noticed_finance_report(start_time,date,self.data_manager.get_all_stockcode())
        return finance_report_data
    
    def set_strategy(self,strategy_class):
        self.strategy=strategy_class(self.account)
        return
    
    def start(self):
        prev_date=self.start_time
        for date in self.marketday_list:
            self.__XRXD(self.account,prev_date,date)
            new_finance_report=self.__get_new_finance_report(prev_date,date)
            self.strategy.handle_report(date,new_finance_report)
            keep_stocks=self.strategy.handle_bar(date)
            self.daily_settlement(date,list(keep_stocks.index))
            prev_date=date

        self.logger.prepare_analysis(self.data_manager.get_baseline(self.start_time,self.end_time))
        analyze_result=self.logger.analyze()
        return analyze_result
    
    def daily_settlement(self,date:datetime,stock_list:list[str]):
        self.account.sell_all(self.data_manager,date,self.logger)
        self.account.estimate_asset(date,self.logger)
        self.account.buyin(self.data_manager,date,stock_list)
        return