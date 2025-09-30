from datetime import datetime, timedelta
import pandas

class BaseStrategy:
    def __init__(self,init_time:datetime):
        return

    
    def handle_bar(self,date:datetime)->pandas.DataFrame:
        return 
    
    def handle_finance_report(self,date:datetime,new_report_stocks:pandas.Index):
        return
    
    def handle_divid(self,date:datetime):
        return
    