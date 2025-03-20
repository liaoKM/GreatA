from ..data.data_manager import LocalDataManager
from ..data.base_manager import FinanceReportType
from ..account import SimpleAccount

import pandas

class BaseStrategy:
    def __init__(self,account:SimpleAccount,data_manager:LocalDataManager,init_time:str):
        self.account_ref=account
        self.data_manager=data_manager
        #history_score:过去4年财务表现
        #score:当前评分，根据市场价格数据刷新
        self.stock_factors=pandas.DataFrame(data=None,columns=['stock_code','profit_per_share','asset_per_share','history_score','score'])


        stocks=self.data_manager.get_all_stockcode()
        init_monitor_data=[]
        for stock_code in stocks:
            report=self.data_manager.get_recent_finance_data(init_time,stock_code,1)
            if self._check_recent_finance_report(report):
                if self._check_history_performance(stock_code):
                    annual_report=self.data_manager.get_recent_finance_data(init_time,stock_code,1,FinanceReportType.ANNUAL)
                    self.stock_factors.loc[stock_code]=[stock_code,0,report.non_gaap_eps,annual_report.net_asset_ps,0]
        return
    
    def update_fractor(self,stock_code,date):
        '''
        连续4年扣非净利润正增长，平均7%以上,允许剔除一年异常
        '''
        report=self.data_manager.get_recent_finance_data(date,stock_code,1)
        return True
    
    def _check_recent_finance_report(self,report:pandas.DataFrame):
        '''
        ROE大于15
        利润同比或环比增长率5以上
        '''
        roe_threshold=15
        recent_profit_gr_threshold=5
        return True

    def _check_history_performance(self,stock_code):

        return
    
    def get_buy_list(self)->list[tuple[str,int]]:
        return [('000001',100)]
    
    def get_sell_list(self)->list[tuple[str,int]]:
        return []
    
    def handle_bar(self,date:str)->tuple[list[tuple[str,int]],list[tuple[str,int]]]:
        return [self.get_buy_list(),self.get_sell_list()]
    
    def handle_report(self,date:str,dataframe:pandas.DataFrame):
        roe_threshold=15
        avg_profit_gr_threshold=10
        recent_profit_gr_threshold=5

        for index,report in dataframe.iterrows():
            if report.roe_non_gaap_wtd<roe_threshold:
                continue
        return