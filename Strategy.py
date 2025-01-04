import pandas
import adata
import datetime

class ActionLogger:
    def __init__(self):
        self.actions:pandas.DataFrame=None
        return
    
    def log_daily(self,date:str,daily_market:pandas.DataFrame,last_buyinto_info:pandas.DataFrame):
        daily_profit=1.0
        if last_buyinto_info is not None:
            dataframe_list=[]
            for stock in last_buyinto_info.itertuples():
                stock_market=daily_market[daily_market['stock_code']==stock.stock_code]
                if stock_market.empty:
                    profit=1.0
                    #todo freeze
                    print("warning:stock {0} error in date:{1}".format(stock.stock_code,date))
                else:
                    profit=float(stock_market.iloc[0].close/stock.price)
                dataframe_list.append((stock.stock_code,date,profit,stock.pe))
            result=pandas.DataFrame(dataframe_list,columns=['stock_code','date','profit','pe'])
            if self.actions is None:
                self.actions=result
            else:
                self.actions=pandas.concat([self.actions,result],ignore_index=True)
        return
    
    def get_daily_avg_info(self):
        if self.actions is not None:
            date_range=self.actions['date'].unique()
            avg_info_list=[]
            for date in date_range:
                avg_info=self.actions[self.actions['date']==date][['profit','pe']].mean()
                avg_info['date']=date
                avg_info_list.append(avg_info)
            return pandas.DataFrame(avg_info_list)
        return
    
    def get_total_acc_info(self):
        if self.actions is not None:
            stocks=self.actions['stock_code'].unique()
            acc_info_list=[]
            for stock_code in stocks:
                stock_action=self.actions[self.actions['stock_code']==stock_code]
                profit=stock_action['profit'].product()
                pe=stock_action['pe'].mean()
                keep_day=len(stock_action)
                acc_info_list.append((stock_code,profit,pow(profit,1/keep_day),pe,keep_day))
            return pandas.DataFrame(acc_info_list,columns=['stock_code','profit','avg_daily_profit','pe','keepday']).sort_values('stock_code')
        return

class BaseStrategy:
    def __init__(self,finance_data:pandas.DataFrame,daily_market_data:pandas.DataFrame):
        self.finance_data=finance_data
        self.daily_market_data=daily_market_data
        self.filtered_stock_list:list[str]=None
        self.action_logger=ActionLogger()

        self.roe_threshold=15.0
        self.profit_gr_threshold=0
        self.recent_profit_gr_threshold=5
        self.profit_gr_avg_threshold=10
        self.select_num=30

        return
    
    def pre_filter(self,year):
        self.filtered_stock_list=[]

        all_stocks=self.finance_data.stock_code.unique()
        stock_list:list[str]=[]
        for stock_code in all_stocks:
            finance_datas=self.finance_data[self.finance_data['stock_code']==stock_code]
            #近两年财报
            bad_finance=False
            profit_gr_last_2_year=0
            recent_annual_financial_report=None
            for finance_year in range(year-3,year-1):
                finance_data=finance_datas[finance_datas['report_date']==str(finance_year)+'-12-31']
                if finance_data.empty:
                    bad_finance=True
                    break
                finance_data=finance_data.iloc[0]
                #年报时间异常：
                if finance_data.notice_date>=(str(year)+'-01-01'):
                    break
                #roe过低
                if finance_data.roe_non_gaap_wtd<self.roe_threshold:
                    bad_finance=True
                    break
                #扣非盈利大额衰减
                if finance_data.non_gaap_net_profit_yoy_gr<self.profit_gr_threshold:
                    bad_finance=True
                    break
                profit_gr_last_2_year+=finance_data.non_gaap_net_profit_yoy_gr
            # 增长乏力
            if profit_gr_last_2_year<self.profit_gr_avg_threshold*2:
                bad_finance=True
            if bad_finance:
                continue
            else:
                stock_list.append(stock_code)

        return stock_list
    
    def __simulate_daily(self,date:str,pre_filtered_list:list[str],last_buyinto_info:pandas.DataFrame):
        market_data=self.daily_market_data[self.daily_market_data['trade_date']==date]
        if market_data.empty:
            return last_buyinto_info
        
        #calc profit
        self.action_logger.log_daily(date,market_data,last_buyinto_info)
        self.action_logger.get_daily_avg_info()
        self.action_logger.get_total_acc_info()

        #strategy
        if pre_filtered_list is None:
            pre_filtered_list=self.finance_data.stock_code.unique()

        dataframe_list=[]

        for stock_code in pre_filtered_list:
            stock_finance=self.finance_data[(self.finance_data['stock_code']==stock_code)&(self.finance_data['notice_date']<date)]
            stock_market=market_data[market_data['stock_code']==stock_code]
            if stock_finance.empty or stock_market.empty:
                continue
                

            #过去两年财报
            bad_finance=False  
            profit_gr_sum=0
            year_finance_reports=stock_finance[stock_finance['report_type']=='年报']
            if len(year_finance_reports)<2:
                continue
            year_finance_reports=year_finance_reports.head(2)
            for finance in year_finance_reports.itertuples():
                if not finance.non_gaap_net_profit_yoy_gr>=self.profit_gr_threshold:
                    bad_finance=True
                    break
                profit_gr_sum+=finance.non_gaap_net_profit_yoy_gr
            if not profit_gr_sum>=self.profit_gr_avg_threshold*2:
                bad_finance=True
            #最近财报
            finance=stock_finance.iloc[0]
            if not finance.non_gaap_net_profit_yoy_gr>=self.profit_gr_avg_threshold:
                bad_finance=True
            if bad_finance:
                continue
            
            #pe
            stock_market=stock_market.iloc[0]
            if finance.report_type=='一季报':
                non_gaap_eps=float(finance.non_gaap_eps*4)
            elif finance.report_type=='中报':
                non_gaap_eps=float(finance.non_gaap_eps*2)
            elif finance.report_type=='三季报':
                non_gaap_eps=float(finance.non_gaap_eps/3*4)
            else:
                non_gaap_eps=finance.non_gaap_eps
            if not finance.non_gaap_eps>0:
                continue
            pe=stock_market.stock_price/non_gaap_eps
            dataframe_list.append([stock_code,pe,stock_market.close])
        stocks=pandas.DataFrame(dataframe_list,columns=['stock_code','pe','price'])
        sorted_stocks=stocks.sort_values('pe')

        return sorted_stocks.head(self.select_num)

    def simulate(self,pre_filtered_list,date_range):
        if pre_filtered_list is None:
            all_stocks=adata.stock.info.all_code()
            pre_filtered_list=list(all_stocks.stock_code)

        self.action_logger=ActionLogger()
        buyinto_info=None
        for date in date_range:
            buyinto_info=self.__simulate_daily(date._date_repr,pre_filtered_list,buyinto_info)
            

        return