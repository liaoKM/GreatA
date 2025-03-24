from .data.data_manager import LocalDataManager
import pandas
class SimpleAccount:
    def __init__(self,init_money):
        self.money=init_money
        self.stocks=pandas.DataFrame(columns=['stock_code','num'],dtype=(str,int)).set_index('stock_code')
        self.buyin_price=pandas.DataFrame(columns=['stock_code','price'],dtype=(str,float)).set_index('stock_code')
        self.profit=pandas.DataFrame(columns=['stock_code','profit'],dtype=(str,float)).set_index('stock_code')
        return
    
    def estimate_asset(self,data_manager:LocalDataManager,date):
        asset=self.money
        if self.stocks.empty==False:
            asset+=(self.stocks['num']*self.buyin_price['price']).sum()
        print("{0}:{1}元".format(date,asset))
        return asset
    
    def sell_all(self,data_manager:LocalDataManager,date:str):
        if self.stocks.empty:
            return
        
        price=data_manager.get_daily_market_data(date,self.stocks.index).close
        success_index=price.index

        self.money+=(price*self.stocks.loc[success_index,'num']).sum()
        profit=(price-self.buyin_price.loc[success_index,'price'])*self.stocks.loc[success_index,'num']
        self.profit=self.profit['profit'].add(profit,fill_value=0).to_frame('profit')

        self.stocks=self.stocks.drop(success_index,axis='index')
        self.buyin_price=self.buyin_price.drop(success_index,axis='index')
        
        if len(self.stocks)!=0:
            print("sell all fail! #remain:{}".format(len(self.stocks)))
        return
    
    def buyin(self,data_manager:LocalDataManager,date:str,stock_list:list[str]):
        if len(stock_list)==0:
            return
        
        avg_money=self.money*min(1/len(stock_list),0.1)
        market_data=data_manager.get_daily_market_data(date,stock_list)

        price=market_data.loc[stock_list].close
        num=(avg_money/price).round(-2).astype(int)
        self.money-=((num*price).sum())
        self.buyin_price=self.buyin_price._append(price.to_frame('price'))
        self.stocks=self.stocks['num'].add(num,fill_value=0).to_frame('num')
        return