from ..data.data_manager import LocalDataManager
from ..account import SimpleAccount
from .. import strategy
from .trade import LocalTrading
    
import pandas
import re
class LocalSimulator:
    def __init__(self,start_time:str,end_time:str,account:SimpleAccount=None):
        self.data_manager=LocalDataManager()
        self.data_manager.init_range(start_time,end_time)
        self.start_time=start_time
        self.end_time=end_time
        if account is not None:
            self.account=account
        else:
            self.account=SimpleAccount(1_000_000)
        
        #交易时间
        self.marketday_list=self.data_manager.get_marketday_list()
        self.strategy=strategy.BaseStrategy(self.account)
        self.trade=LocalTrading(self.data_manager)
        return
    
    def __XRXD(self,account:SimpleAccount,date:str):
        '''除权除息'''
        xrxd_datas=self.data_manager.get_xrxd_data(date,date)
        for index,data in xrxd_datas.iterrows():
            if data.stock_code in account.stocks.keys():
                xr_pattern='10股转赠(.*?)股'
                xd_pattern='10股派(.*?)元'
                right=re.search(xr_pattern,data.dividend_plan)
                dividend=re.search(xd_pattern,data.dividend_plan)
                if (right is None) and (dividend is None):
                    print("[Warning]:{0} xrxd data missing!",data.stock_code)
                takes_num=account.stocks[data.stock_code]
                account.money+=int(takes_num/10)*dividend
                account.stocks[data.stock_code]+=int(takes_num/10)*right
        return
    
    def __get_new_finance_report(self,prev_date,date)->pandas.DataFrame:
        finance_report_data=self.data_manager.get_noticed_finance_report(prev_date,date)
        return finance_report_data
    
    def set_strategy(self,strategy_class):
        self.strategy=strategy_class(self.account)
        return
    
    def start(self):
        prev_date=self.start_time
        for date in self.marketday_list:
            self.__XRXD(self.account,date)
            new_finance_report=self.__get_new_finance_report(prev_date,date)
            self.strategy.handle_report(date,new_finance_report)
            buy_list,sell_list=self.strategy.handle_bar(date,self.data_manager)
            self.daily_settlement(date,buy_list,sell_list)
            prev_date=date
        return
    
    def daily_settlement(self,date:str,buy_list:list[tuple[str,int]],sell_list:list[tuple[str,int]]):
        for stock,num in sell_list:
            result=self.trade.order(self.account,stock,-num,date)
            if result==False:
                print("[Warning]:order fail!")
        for stock,num in buy_list:
            result=self.trade.order(self.account,stock,num,date)
            if result==False:
                print("[Warning]:order fail!")

        return