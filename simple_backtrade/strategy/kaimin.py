from ..data.data_manager import LocalDataManager
from ..data.base_manager import FinanceReportType
from ..account import SimpleAccount

import pandas
import numpy as np
import math
from datetime import datetime, timedelta

class BaseStrategy:
    def __init__(self,account:SimpleAccount,data_manager:LocalDataManager,init_time:str):
        self.account_ref=account
        self.data_manager=data_manager
        self.stock_factors=pandas.DataFrame(data=None,columns=['profit_per_share','asset_per_share','history_score'],dtype=float)
        
        stocks=self.data_manager.get_all_stockcode()
        stocks=self._check_recent_finance_report(stocks,init_time)
        history_score=self._get_history_score(stocks,init_time)
        self._update_fractor(history_score.index,init_time,history_score)
        return
    
    def _update_fractor(self,stocks:list[str],date:datetime,history_score:pandas.DataFrame):
        if history_score is None:
            history_score=self._get_history_score(stocks,date)
            stocks=history_score.index
        report=self.data_manager.get_recent_finance_data(date,stocks,1).droplevel(0).copy()
        annual_report=self.data_manager.get_recent_finance_data(date,stocks,1,FinanceReportType.ANNUAL).droplevel(0).copy()
        report.fillna({'non_gaap_eps':report.basic_eps},inplace=True)
        annual_report.fillna({'non_gaap_eps':annual_report.basic_eps},inplace=True)
        report.fillna({'net_asset_ps':annual_report.net_asset_ps},inplace=True)

        #fix eps
        mapping={
            FinanceReportType.S1.value:1.0/4.0,
            FinanceReportType.S2.value:2.0/4.0,
            FinanceReportType.S3.value:3.0/4.0,
            FinanceReportType.S4.value:1.0,
        }
        season_weight=report.report_type.map(mapping)
        report.non_gaap_eps = report.non_gaap_eps + annual_report.non_gaap_eps * (1-season_weight)

        stock_factors=pandas.concat([report.non_gaap_eps,report.net_asset_ps,history_score],axis=1)
        stock_factors.columns=['profit_per_share','asset_per_share','history_score']
        self.stock_factors=pandas.concat([self.stock_factors,stock_factors],axis=0)
        return 
    
    def _check_recent_finance_report(self,stocks,date):
        roe_threshold=10
        recent_profit_gr_threshold=10
        leverage_threshold=70

        reports=self.data_manager.get_recent_finance_data(date,stocks,1).copy().droplevel(level='notice_date')
        annual_reports=self.data_manager.get_recent_finance_data(date,stocks,1,FinanceReportType.ANNUAL).droplevel(level='notice_date')
        reports.fillna({'roe_non_gaap_wtd':reports.roe_wtd},inplace=True)
        reports["roe_non_gaap_wtd"] = reports.roe_non_gaap_wtd.where(reports.roe_non_gaap_wtd != 0, reports.roe_wtd)
        annual_reports.fillna({'roe_non_gaap_wtd':annual_reports.roe_wtd},inplace=True)
        annual_reports["roe_non_gaap_wtd"] = annual_reports.roe_non_gaap_wtd.where(annual_reports.roe_non_gaap_wtd != 0, annual_reports.roe_wtd)
        reports.fillna({'non_gaap_net_profit_yoy_gr':reports.net_profit_yoy_gr},inplace=True)
        reports.fillna({'asset_liab_ratio':annual_reports.asset_liab_ratio},inplace=True)

        #fix roe
        mapping={
            FinanceReportType.S1.value:1.0/4.0,
            FinanceReportType.S2.value:2.0/4.0,
            FinanceReportType.S3.value:3.0/4.0,
            FinanceReportType.S4.value:1.0,
        }
        season_weight=reports.report_type.map(mapping)
        reports.roe_non_gaap_wtd=reports.roe_non_gaap_wtd+annual_reports.roe_non_gaap_wtd*(1-season_weight)

        filtered=reports[(reports['roe_non_gaap_wtd']>=roe_threshold)&(reports.non_gaap_net_profit_yoy_gr>=recent_profit_gr_threshold)&(reports.asset_liab_ratio<=leverage_threshold)]
        return filtered.index.get_level_values('stock_code').unique()

    def _get_history_score(self,stocks,date):
        roe_threshold=10
        profit_grows_mean_threshold=10
        profit_grows_min_threshold=-30


        annual_reports=self.data_manager.get_recent_finance_data(date,stocks,3,FinanceReportType.ANNUAL).copy()
        annual_reports=annual_reports.dropna(subset='non_gaap_net_profit_yoy_gr')
        annual_reports.fillna({'roe_non_gaap_wtd':annual_reports.roe_wtd},inplace=True)
        annual_reports["roe_non_gaap_wtd"] = annual_reports.roe_non_gaap_wtd.where(annual_reports.roe_non_gaap_wtd != 0, annual_reports.roe_wtd)
        #roe filter
        stock_roe_min=annual_reports.roe_non_gaap_wtd.groupby(level='stock_code').min()
        stocks=stock_roe_min[stock_roe_min>=roe_threshold].index
        annual_reports=annual_reports.loc[pandas.IndexSlice[:,stocks],:]

        #num filter
        stock_code_report_num=annual_reports.groupby(level='stock_code').size()
        stocks=stock_code_report_num[stock_code_report_num>=3].index
        annual_reports=annual_reports.loc[pandas.IndexSlice[:,stocks],:]
        #min filter
        stock_yoy_gr_min=annual_reports.non_gaap_net_profit_yoy_gr.groupby(level='stock_code').min()
        stocks=stock_yoy_gr_min[stock_yoy_gr_min>=profit_grows_min_threshold].index
        annual_reports=annual_reports.loc[pandas.IndexSlice[:,stocks],:]
        #increase filter
        decrease_num=(annual_reports.non_gaap_net_profit_yoy_gr<0).groupby(level='stock_code').sum()
        stocks=decrease_num[decrease_num<=0].index
        annual_reports=annual_reports.loc[pandas.IndexSlice[:,stocks],:]
        #mean filter
        stock_yoy_gr_mean=annual_reports.non_gaap_net_profit_yoy_gr.groupby(level='stock_code').mean()
        stock_yoy_gr_mean=stock_yoy_gr_mean[stock_yoy_gr_mean>=profit_grows_mean_threshold]

        return stock_yoy_gr_mean
    
    def handle_bar(self,date:datetime)->pandas.DataFrame:
        keeps_num=10

        baseline_data=self.data_manager.get_recent_baseline(date,61).close
        baseline=baseline_data.iloc[-1]
        base_m20_t1=baseline_data.iloc[-2-20:-2].mean()
        base_m20_t0=baseline_data.iloc[-1-20:-1].mean()
        base_m60_t1=baseline_data.head(60).mean()
        base_m60_t0=baseline_data.tail(60).mean()
        baseline_negative=(base_m60_t0>base_m20_t0 and base_m20_t0>baseline and base_m20_t1>base_m20_t0 and base_m60_t1>base_m60_t0)
        if baseline_negative:
            return pandas.DataFrame()


        market_data=self.data_manager.get_daily_market_data(date,self.stock_factors.index)
        if len(market_data)==0:
            breakpoint()#error debug
            return None
        
        valid_stock=self.stock_factors.index.intersection(market_data.index)
        share_price=market_data.loc[valid_stock].close
        history_price=self.data_manager.get_recent_stock_market_data(valid_stock,date,60+1)['close']
        m60_t0=history_price.groupby(level='stock_code').tail(60).groupby(level='stock_code').mean()
        m60_t1=history_price.groupby(level='stock_code').head(60).groupby(level='stock_code').mean()
        history_price=self.data_manager.get_recent_stock_market_data(valid_stock,date,20+1)['close']
        m20_t0=history_price.groupby(level='stock_code').tail(20).groupby(level='stock_code').mean()
        m20_t1=history_price.groupby(level='stock_code').head(20).groupby(level='stock_code').mean()
        share_price=share_price.reindex(self.stock_factors.index,fill_value=1e3)
        m60_t1=m60_t1.reindex(self.stock_factors.index,fill_value=1e9)
        m60_t0=m60_t0.reindex(self.stock_factors.index,fill_value=1e9)
        m20_t1=m20_t1.reindex(self.stock_factors.index,fill_value=1e9)
        m20_t0=m20_t0.reindex(self.stock_factors.index,fill_value=1e9)
        pe=share_price/self.stock_factors['profit_per_share']

        predict_pe=pe/(1.0+self.stock_factors.history_score/100)
        #predict_pe=pe

        low_estimat_pe=predict_pe[predict_pe<30]
        recovering_pe=predict_pe[(predict_pe<30)&((m60_t1<m60_t0)&(m20_t1<m20_t0)&(m60_t0<m20_t0))].sort_values()

        return recovering_pe.head(keeps_num)
    
    def handle_report(self,date:str,dataframe:pandas.DataFrame):
        stocks=dataframe.index.get_level_values('stock_code').unique()
        self.stock_factors.drop(index=stocks.intersection(self.stock_factors.index),inplace=True)
        if len(self.stock_factors)==0:
            breakpoint()
        stocks=self._check_recent_finance_report(stocks,date)
        history_score=self._get_history_score(stocks,date)
        self._update_fractor(history_score.index,date,history_score)
        return