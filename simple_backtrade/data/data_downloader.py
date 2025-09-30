import datetime
class DataDownloader:

    def __init__(self,output_path:str="./downloaded"):
        self.output_path=output_path
        self.stocks=None

        self.market_file_format="market_data_{}.parquet"
        self.finance_file_format="finance_data_{}.csv"
        self.divid_file_format="divid_data_{}.csv"
        self.date_fromat="%Y-%m-%d"
        self.market_file=None
        self.finance_file=None
        self.divid_file=None



        return
    
    def update(self,date:datetime.datetime)->None:
        if(self.check(date)):
            return
        else:
            self.clear_file()
            self.download(date)
            assert(self.check(date))
        return
    
    def check(self,date:datetime.datetime)->bool:
        return
    
    def download(self,data:datetime.datetime):
        return
        
    def clear_file(self)->None:
        return
    