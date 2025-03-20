from ..account import SimpleAccount
from ..data.data_manager import LocalDataManager
class LocalTrading:
    def __init__(self,data_manager:LocalDataManager):
        self.data_manager=data_manager
        return
    
    
    
    def order(self,account:SimpleAccount,stock_code:str,num:int,date:str)->bool:

        return True