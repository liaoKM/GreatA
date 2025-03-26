from ..data.data_manager import LocalDataManager
from ..data.base_manager import FinanceReportType
from ..account import SimpleAccount

import pandas
import numpy as np
import math

class BaseStrategy:
    def __init__(self,account:SimpleAccount,data_manager:LocalDataManager,init_time:str,use_cached_data:bool=False):
        self.account_ref=account
        self.data_manager=data_manager
        #history_score:过去4年财务表现
        #score:当前评分，根据市场价格数据刷新
        self.stock_factors=pandas.DataFrame(data=None,columns=['stock_code','profit_per_share','asset_per_share','history_score','pe','rising'])

        if use_cached_data:
            data_types={'index':str,'stock_code':str}
            try:
                self.stock_factors=pandas.read_csv("./cached_fractors{}.csv".format(init_time),dtype=data_types,index_col='index')
            except:
                pass
        
        if len(self.stock_factors)==0:
            stocks=self.data_manager.get_all_stockcode()
            init_monitor_data=[]
            for stock_code in stocks:
                if self._check_recent_finance_report(stock_code,init_time):
                    history_score=self._get_history_score(stock_code,init_time)
                    if history_score>0:
                        self._update_fractor(stock_code,init_time,history_score)
            if use_cached_data:
                self.stock_factors.index.name='index'
                self.stock_factors.to_csv("./cached_fractors{}.csv".format(init_time))
        
        return
    
    def _update_fractor(self,stock_code,date,history_score):
        if history_score is None:
            history_score=self._get_history_score(stock_code,date)
        report=self.data_manager.get_recent_finance_data(date,stock_code,1).iloc[0]

        profit_per_share=report.non_gaap_eps
        if np.isnan(profit_per_share):
            profit_per_share=report.basic_eps

        asset_per_share=report.net_asset_ps
        if np.isnan(asset_per_share):
            report=self.data_manager.get_recent_finance_data(date,stock_code,1,FinanceReportType.ANNUAL).iloc[0]
            asset_per_share=report.net_asset_ps

        if np.isnan(profit_per_share) or np.isnan(asset_per_share):
            print("stock {}: missing net_asset_ps or non_gaap_eps".format(stock_code))
            return
        if profit_per_share<0:
            #wtf? roe为正，实际亏损？？why??
            return
        
        if report.report_type==FinanceReportType.S1.value:
            weighted_profit_per_share=profit_per_share*4
        elif report.report_type==FinanceReportType.S2.value:
            weighted_profit_per_share=profit_per_share*2
        elif report.report_type==FinanceReportType.S3.value:
            weighted_profit_per_share=profit_per_share/3*4
        else:
            weighted_profit_per_share=profit_per_share

        self.stock_factors.loc[stock_code]=[stock_code,weighted_profit_per_share,asset_per_share,history_score,100,False]
        return True
    
    def _check_recent_finance_report(self,stock_code,date):
        roe_threshold=15
        recent_profit_gr_threshold=5

        report=self.data_manager.get_recent_finance_data(date,stock_code,1)
        try:
            report=report.iloc[0]
        except:
            print("stock {}: missing finance data".format(stock_code))
            return False
        
        #盈利
        roe=report.roe_non_gaap_wtd
        profit_grow=report.non_gaap_net_profit_yoy_gr
        if np.isnan(roe):
            roe=report.roe_wtd
        if np.isnan(profit_grow):
            profit_grow=report.net_profit_yoy_gr 

        if np.isnan(profit_grow):
            #calc profit grow manually
            print("stock {}: missing profit_yoy_gr data".format(stock_code))
            return False
        if np.isnan(roe):
            #print("stock {}: missing roe data".format(report.stock_code))
            return False

        if roe < roe_threshold:
            return False
        if profit_grow < recent_profit_gr_threshold:
            return False
        
        #杠杆
        #todo 银行可以高杠杆
        if np.isnan(report.asset_liab_ratio):
            #取最近年报的资产负债率
            report=self.data_manager.get_recent_finance_data(date,stock_code,1,FinanceReportType.ANNUAL)
        if np.isnan(report.asset_liab_ratio) or report.asset_liab_ratio>70:#资产负债率
            return False

        return True

    def _get_history_score(self,stock_code,date):
        profit_grows_threshold=10

        annual_report=self.data_manager.get_recent_finance_data(date,stock_code,4,FinanceReportType.ANNUAL)
        profit_grows=annual_report['non_gaap_net_profit_yoy_gr']
        if len(profit_grows)==4:#4年允许一年异常
            if (profit_grows<0).sum()>1:
                return 0
        elif len(profit_grows)==3:#新股连续3年增长
            if (profit_grows<0).sum()>0:
                return 0
        else:
            return 0

        if profit_grows.min()<-30:
            return 0
        if profit_grows.mean()<profit_grows_threshold:
            return 0
        
        # if profit_grows.mean()>100:
        #     breakpoint()#异常值
        
        return profit_grows.mean()
    
    def handle_bar(self,date:str)->pandas.DataFrame:
        keeps_num=20

        baseline=self.data_manager.get_recent_baseline(date,1).iloc[0].close
        baseline_m400=self.data_manager.get_recent_baseline(date,400).close.mean()
        baseline_negative=(baseline<baseline_m400)


        market_data=self.data_manager.get_daily_market_data(date,self.stock_factors.index)
        if len(market_data)==0:
            breakpoint()#error debug
            return None

        for stock_code in self.stock_factors.index:
            try:
                share_price=market_data.loc[stock_code].close
                mean20=self.data_manager.get_recent_stock_market_data(stock_code,date,20)['close'].mean()
            except:
                #print('{} stock-{} 停牌'.format(date,stock_code))
                share_price=1e3
                mean20=1e9
            pe=share_price/self.stock_factors.loc[stock_code]['profit_per_share']
            if np.isnan(pe) or pe<=0:
                breakpoint()#error debug
            
            self.stock_factors.at[stock_code,'pe']=pe
            self.stock_factors.at[stock_code,'rising']=(share_price>=mean20)

        low_estimat=self.stock_factors[self.stock_factors['pe']<35]
        rising_low=low_estimat[low_estimat['rising']==True]
        keep_stocks=rising_low.sort_values(by='pe').head(keeps_num)
        strategy_negative=(len(rising_low)/len(low_estimat)<0.3)
        strategy_positive=(len(rising_low)/len(low_estimat)>0.6)
        if strategy_positive:
            if baseline_negative:
                print("{}: baseline negative strategy positive".format(date))
            return keep_stocks
        # elif strategy_negative:
        #     print("{}: strategy negative".format(date))
        #     return pandas.DataFrame()
        else:
            if baseline_negative:
                print("{}: baseline negative".format(date))
                return pandas.DataFrame()
            else:
                return keep_stocks

        return keep_stocks
    
    def handle_report(self,date:str,dataframe:pandas.DataFrame):
        roe_threshold=15
        avg_profit_gr_threshold=10
        recent_profit_gr_threshold=5

        if dataframe is None:
            return

        for stock_code in dataframe.index:
            if stock_code in self.stock_factors:
                self.stock_factors.drop(index=stock_code)
            if self._check_recent_finance_report(stock_code,date):
                history_score=self._get_history_score(stock_code,date)
                if history_score>0:
                    self._update_fractor(stock_code,date,history_score)
        return