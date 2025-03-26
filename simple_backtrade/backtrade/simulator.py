from ..data.data_manager import LocalDataManager
from ..account import SimpleAccount
from .. import strategy
from ..log import TradeLogger
    
import pandas
import re
from datetime import datetime, timedelta

class LocalSimulator:
    def __init__(self,start_time:str,end_time:str,account:SimpleAccount=None):
        self.data_manager=LocalDataManager()
        data_start_time=(datetime.strptime(start_time, "%Y-%m-%d")-timedelta(days=30)).strftime("%Y-%m-%d")
        self.data_manager.init_range(data_start_time,end_time)
        self.start_time=start_time
        self.end_time=end_time
        if account is not None:
            self.account=account
        else:
            self.account=SimpleAccount(1_000_000)
        
        #交易时间
        self.marketday_list=self.data_manager.get_marketday_list(self.start_time,self.end_time)
        self.strategy=strategy.BaseStrategy(self.account,self.data_manager,start_time,True)
        self.logger=TradeLogger()
        return
    
    def __XRXD(self,account:SimpleAccount,prev_date,date:str):
        '''除权除息'''
        start_time=(datetime.strptime(prev_date, "%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
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
                takes_num=account.stocks.loc[data.stock_code]
                if len(dividend)==1:
                    account.money+=int(takes_num/10)*float(dividend[0])
                if len(right)==1:
                    account.stocks.at[data.stock_code,'num']+=int(int(takes_num/10)*float(right[0]))
        return
    
    def __get_new_finance_report(self,prev_date,date)->pandas.DataFrame:
        start_time=(datetime.strptime(prev_date, "%Y-%m-%d")+timedelta(days=1)).strftime("%Y-%m-%d")
        finance_report_data=self.data_manager.get_noticed_finance_report(start_time,date)
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
        self.logger.analyze()
        return
    
    def daily_settlement(self,date:str,stock_list:list[str]):
        
        self.account.sell_all(self.data_manager,date,self.logger)
        self.account.estimate_asset(self.data_manager,date,self.logger)
        self.account.buyin(self.data_manager,date,stock_list)
        return