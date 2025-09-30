from .data_downloader import DataDownloader
import tqdm
import time
import pandas
from multiprocessing import Pool
import os
import datetime
from xtquant import xtdata
import re



class XtDataDownloader(DataDownloader):
    
    def check(self,date:datetime.datetime)->bool:
        pattern_market=r'(.*)_data_(.*)\.(.*)'
        latest:dict[str,datetime.datetime]={}
        for root, dirs, files in os.walk(self.output_path): 
            for filename in files:
                match = re.search(pattern_market, filename)
                if match:
                    file_type=match.group(1)
                    file_date=datetime.datetime.strptime(match.group(2),self.date_fromat)
                    if file_type not in latest.keys():
                        latest[file_type]=file_date
                    elif latest[file_type]<file_date:
                        latest[file_type]=file_date
                    else:
                        pass
        
        out_of_date=True
        if latest.get('market',datetime.datetime(1,1,1))>=date and latest.get('finance',datetime.datetime(1,1,1))>=date and latest.get('divid',datetime.datetime(1,1,1))>=date:
            out_of_date=False

        if out_of_date==False:
            self.market_file="{}_data_{}.parquet".format("market",latest["market"].strftime(self.date_fromat))
            self.finance_file="{}_data_{}.csv".format("finance",latest["finance"].strftime(self.date_fromat))
            self.divid_file="{}_data_{}.csv".format("divid",latest["divid"].strftime(self.date_fromat))

        return not out_of_date
    
    def download(self,date:datetime.datetime):
        #self._download_rawdata()
        self._rawdata_to_pandas(None,date)
        self.market_data.to_parquet(os.path.join(self.output_path,self.market_file_format.format(date.strftime(self.date_fromat))))
        self.finance_data.to_csv(os.path.join(self.output_path,self.finance_file_format.format(date.strftime(self.date_fromat))))
        self.divid_data.to_csv(os.path.join(self.output_path,self.divid_file_format.format(date.strftime(self.date_fromat))))
        return
        
    def clear_file(self)->None:
        #todo
        return
    


    def _download_rawdata(self):
        #指数数据
        xtdata.download_index_weight()

        stocks = xtdata.get_stock_list_in_sector('沪深京A股')
        stocks.append('000300.SH')
        self.stocks=stocks
        
        #财报
        xtdata.download_financial_data(self.stocks, table_list=[])
        
        #k线
        progress=tqdm.tqdm(self.stocks)
        progress.set_description("Downloading")
        for s in progress:
            xtdata.download_history_data(s, '1d', '', '')
        return
    
    def _rawdata_to_pandas(self,start_time:datetime.datetime,end_time=datetime.datetime):
        if self.stocks is None:
            self.stocks=xtdata.get_stock_list_in_sector('沪深京A股')
            self.stocks.append('000300.SH')#沪深300
        self._init_market_data(self.stocks,start_time,end_time)#K线数据
        self._init_divid_data(self.stocks,start_time,end_time)#分红送股数据
        self._init_finance_reports(self.stocks,start_time,end_time)#财报数据
        return
    
    def _init_finance_reports(self,stocks:list[str],start_time:datetime.datetime,end_time:datetime.datetime):
        start_time_str=''
        end_time_str=''
        if start_time is not None:
            start_time_str=start_time.strftime('%Y%m%d')
        if end_time is not None:
            end_time_str=end_time.strftime('%Y%m%d')
        data=xtdata.get_financial_data(stocks,["PershareIndex"],start_time_str,end_time_str)
        finance_report_list=[]
        annual_finance_report_list=[]
        for stock_code,finance_data in data.items():
            PershareIndex=finance_data['PershareIndex']
            if PershareIndex.empty==False:
                #剔除异常财报：公告时间>>财报时间，可能是上市后发布的早期财报
                PershareIndex.rename(columns={'m_timetag': 'm_reptime'}, inplace=True)
                PershareIndex['m_anntime']=pandas.to_datetime(PershareIndex.m_anntime)
                PershareIndex['m_reptime']=pandas.to_datetime(PershareIndex.m_reptime)
                PershareIndex=PershareIndex[PershareIndex.m_anntime-PershareIndex.m_reptime<datetime.timedelta(31*4)].copy()
                PershareIndex['stock_code']=stock_code
                PershareIndex.drop_duplicates(subset='m_reptime', keep='last', inplace=True)
                finance_report_list.append(PershareIndex)

        self.finance_data=pandas.concat(finance_report_list)
        self.finance_data=self.finance_data.set_index(['stock_code','m_reptime'])
        return
    
    def _init_market_data(self,stocks:list[str],start_time:datetime.datetime,end_time:datetime.datetime):
        start_time_str=''
        end_time_str=''
        if start_time is not None:
            start_time_str=start_time.strftime('%Y%m%d')
        if end_time is not None:
            end_time_str=end_time.strftime('%Y%m%d')
        data=xtdata.get_market_data_ex(stock_list=stocks,start_time=start_time_str,end_time=end_time_str)
        market_data_list=[]
        for stock_code,market_data in data.items():
            market_data.index.rename('date',inplace=True)
            market_data.index=pandas.to_datetime(market_data.index)
            market_data['stock_code']=stock_code
            market_data['m20'] = market_data['close'].rolling(window=20).mean()
            market_data['m60'] = market_data['close'].rolling(window=60).mean()
            market_data.reset_index(inplace=True)
            market_data.set_index(['date','stock_code'],inplace=True)
            market_data_list.append(market_data)
        self.market_data=pandas.concat(market_data_list).sort_index()
            
        return
    
    def _init_divid_data(self,stocks:list[str],start_time:datetime.datetime,end_time:datetime.datetime):
        divid_data_list=[]
        for stock_code in stocks:
            divid_data=xtdata.get_divid_factors(stock_code)
            divid_data['stock_code']=stock_code
            divid_data.index.rename('date',inplace=True)
            divid_data.index=pandas.to_datetime(divid_data.index)
            divid_data.reset_index(inplace=True)
            divid_data.set_index(['date','stock_code'],inplace=True)
            divid_data_list.append(divid_data)
        self.divid_data=pandas.concat(divid_data_list).sort_index()
        return
    