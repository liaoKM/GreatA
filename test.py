from simple_backtrade.data.xt_data_downloader import XtDataDownloader
from simple_backtrade.data.data_manager import DataManager
from simple_backtrade.strategy.xt_kaimin import MyStrategy
import datetime
if __name__=='__main__':
    downloader=XtDataDownloader()
    data_manager=DataManager(downloader,datetime.datetime(2025,1,1),datetime.datetime(2025,9,1))
    strategy=MyStrategy(data_manager,datetime.datetime(2025,1,1),datetime.datetime(2025,9,1))
    pass