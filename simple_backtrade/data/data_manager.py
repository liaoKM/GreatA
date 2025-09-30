from enum import Enum
from .data_downloader import DataDownloader
import pandas
import datetime
import os


class DataManager:
    def __init__(self,downloader:DataDownloader,start_time:datetime.datetime,end_time:datetime.datetime):
        downloader.update(end_time)

        self.divid_data=pandas.read_csv(os.path.join(downloader.output_path,downloader.divid_file),index_col=['date','stock_code'],parse_dates=['date',])

        self.market_data=pandas.read_parquet(os.path.join(downloader.output_path,downloader.market_file))
        self.market_data:pandas.DataFrame=self.market_data.loc[start_time:end_time,:].copy()

        self.finance_data=pandas.read_csv(os.path.join(downloader.output_path,downloader.finance_file),index_col=['stock_code','m_reptime'],parse_dates=['m_reptime', 'm_anntime'])
        self.annual_finance_data=self.finance_data.loc[self.finance_data.index.get_level_values('m_reptime').month==12]

        return
    
#todo LocalDataManager
#todo XtDataManager