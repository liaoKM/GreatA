import pandas
from datetime import datetime, timedelta
from ..data.data_manager import DataManager
from .base import BaseStrategy


class MyStrategy(BaseStrategy):
    def __init__(self,data_manager:DataManager):
        self.data=data_manager
        #init factor
        self.lst_report_time=None
        self.stock_factors=pandas.DataFrame(data=None,columns=['profit_per_share','asset_per_share','history_profit_growth_rate'],dtype=float)
        return
    
    def _check_recent_finance_report(self,stocks:list[str],start_time:datetime,end_time:datetime):
        '''
        [start_time,end_time)
        '''
        recent_profit_gr_threshold=10
        leverage_threshold=70

        stocks=self.data.finance_data.index.get_level_values('stock_code').intersection(stocks)
        reports=self.data.finance_data.loc[stocks]
        if start_time is None:
            reports=reports[(reports.m_anntime<=end_time)]
        else:
            reports=reports[(reports.m_anntime>start_time)&(reports.m_anntime<=end_time)]

        recent_reports=reports.groupby(level='stock_code').tail(1)

        filtered=recent_reports[(recent_reports.adjusted_net_profit_rate>=recent_profit_gr_threshold)&(recent_reports['gear_ratio']<leverage_threshold)]
        return filtered.index.get_level_values('stock_code')
    
    def _get_history_profit_growth_rate(self,stocks,start_time:datetime):
        roe_threshold=10
        profit_grows_mean_threshold=10
        profit_grows_min_threshold=-30
        num=3

        stocks=self.data.annual_finance_data.index.get_level_values('stock_code').intersection(stocks)
        annual_reports=self.data.annual_finance_data.loc[stocks,:]
        annual_reports=annual_reports[annual_reports.m_anntime<start_time]
        annual_reports=annual_reports.groupby(level='stock_code').tail(num)

        #num filter
        stock_code_report_num=annual_reports.groupby(level='stock_code').size()
        stocks=stock_code_report_num[stock_code_report_num>=num].index
        annual_reports=annual_reports.loc[stocks,:]

        #roe filter
        history_roe_min=annual_reports.net_roe.groupby(level='stock_code').min()
        stocks=history_roe_min[history_roe_min>=roe_threshold].index
        annual_reports=annual_reports.loc[stocks,:]

        #无重大亏损年份
        stock_yoy_gr_min=annual_reports.adjusted_net_profit_rate.groupby(level='stock_code').min()
        stocks=stock_yoy_gr_min[stock_yoy_gr_min>=profit_grows_min_threshold].index
        annual_reports=annual_reports.loc[stocks,:]
        #increase filter
        decrease_num=(annual_reports.adjusted_net_profit_rate<0).groupby(level='stock_code').sum()
        stocks=decrease_num[decrease_num<=0].index
        annual_reports=annual_reports.loc[stocks,:]
        #mean filter
        stock_yoy_gr_mean=(annual_reports.adjusted_net_profit_rate.groupby(level='stock_code').sum()-annual_reports.adjusted_net_profit_rate.groupby(level='stock_code').max())/(num-1)
        stock_yoy_gr_mean=stock_yoy_gr_mean[stock_yoy_gr_mean>=profit_grows_mean_threshold]

        return stock_yoy_gr_mean

    def _update_fractor(self,stocks:list[str],date:datetime,history_profit_growth_rate:pandas.DataFrame):
        if history_profit_growth_rate is None:
            history_profit_growth_rate=self._get_history_profit_growth_rate(stocks,date)
            stocks=history_profit_growth_rate.index

        report=self.data.finance_data.loc[stocks,:].copy()
        annual_report=self.data.annual_finance_data.loc[stocks,:]
        report=report[(report.m_anntime)<date].groupby(level='stock_code').tail(1)
        annual_report=annual_report[(annual_report.m_anntime)<date].groupby(level='stock_code').last()
        mapping={
            3:1.0/4.0,
            6:2.0/4.0,
            9:3.0/4.0,
            12:1.0,
        }
        season_weight=report.index.get_level_values('m_reptime').month.map(mapping)
        report=report.groupby(level='stock_code').last()
        report.fillna({'adjusted_earnings_per_share':report.s_fa_eps_basic},inplace=True)
        report.adjusted_earnings_per_share = report.adjusted_earnings_per_share + annual_report.adjusted_earnings_per_share * (1-season_weight)

        history_profit_growth_rate=report.adjusted_net_profit_rate*0.5+history_profit_growth_rate*0.5

        stock_factors=pandas.concat([report.adjusted_earnings_per_share,report.s_fa_bps,history_profit_growth_rate],axis=1)
        stock_factors.columns=['profit_per_share','asset_per_share','history_profit_growth_rate']
        self.stock_factors=pandas.concat([self.stock_factors,stock_factors],axis=0)
        return 
    
    
    def handle_bar(self,date:datetime)->pandas.DataFrame:

        #K线
        if date not  in self.data.market_data.index.get_level_values('date'):
            #闭市
            return
        cur = self.data.market_data.loc[date,'000300.SH']
        prev = self.data.market_data.loc[pandas.IndexSlice[:date-timedelta(1),'000300.SH'],:].iloc[-1]
        baseline_negative=(prev.m60>cur.m60 and prev.m60>cur.close)
        if baseline_negative:
             return pandas.DataFrame()


        market_data=self.data.market_data.loc[date,:]
        stocks=self.stock_factors.index.intersection(market_data.index)
        market_data=market_data.loc[stocks]
        if len(stocks)==0:
            breakpoint()#error debug
            return None
        
        share_price=market_data.close
        mean20=market_data.m20
        mean60=market_data.m60
        pe=share_price/self.stock_factors['profit_per_share']
        predict_pe=pe/((1.0+self.stock_factors.history_profit_growth_rate/100)*(1.0+self.stock_factors.history_profit_growth_rate/100))#预测两年后pe
        low_estimat_pe=predict_pe[predict_pe<20]
        recovering_pe=predict_pe[(predict_pe<20)&(share_price>=mean20)].sort_values()
        return recovering_pe.head(20)

    
    def handle_report(self,date:datetime,new_report_stocks:pandas.Index)->None:
        # if new_report_stocks is None:
        #     new_report_stocks=self.data.finance_data[(self.data.finance_data.m_anntime>=self.lst_report_time) & (self.data.finance_data.m_anntime<date)].index.get_level_values('stock_code')
        if new_report_stocks.empty:
            return
        self.stock_factors.drop(index=new_report_stocks.intersection(self.stock_factors.index),inplace=True)
        stocks=self._check_recent_finance_report(new_report_stocks,self.lst_report_time,date)
        history_profit_growth_rate=self._get_history_profit_growth_rate(stocks,date)
        self._update_fractor(history_profit_growth_rate.index,date,history_profit_growth_rate)
        self.lst_report_time=date
        return
    
    def handel_devid(self,date:datetime)->None:
        #todo送股配股会影响每股净资产和每股净利润
        return