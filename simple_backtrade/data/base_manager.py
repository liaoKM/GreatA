from enum import Enum

class FinanceReportType(Enum):
    ANY=''
    S1='03-31'
    S2='06-30'
    S3='09-30'
    S4='12-31'
    MID='06-30'
    ANNUAL='12-31'
    


class BaseManager:
    def __init__(self):
        return
    
    def init_range(self,start_time,end_time,stocks=[]):
        return
    
    def get_daily_market_data(self,date,stocks=[]):
        return
    
    def get_stock_market_data(self,stock_code,start_time,end_time):
        return
    
    def get_recent_stock_market_data(self,stock_code,now_date,count=1):
        return

    def get_recent_finance_data(self,stock_code,now_date,count=1,type:FinanceReportType=FinanceReportType.ANY):
        return
    
    def get_noticed_finance_report(self,start_time,end_time,stocks=[]):
        return