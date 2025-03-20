from ..data.data_manager import LocalDataManager
from ..account import SimpleAccount

import pandas
class BaseStrategy:
    def __init__(self,account:SimpleAccount):
        self.account_ref=account
        return
    
    def get_buy_list(self)->list[tuple[str,int]]:
        return [('000001',100)]
    
    def get_sell_list(self)->list[tuple[str,int]]:
        return []
    
    def handle_bar(self,date:str,data_manager:LocalDataManager)->tuple[list[tuple[str,int]],list[tuple[str,int]]]:
        return [self.get_buy_list(),self.get_sell_list()]
    
    def handle_report(self,date:str,dataframe:pandas.DataFrame):
        return