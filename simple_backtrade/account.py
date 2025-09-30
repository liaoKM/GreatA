from .data.data_manager import DataManager
from .log import TradeLogger

import pandas
from datetime import datetime,timedelta

class SimpleAccount:
    def __init__(self,init_money:int):
        self.money=init_money
        self.stocks=pandas.DataFrame(columns=['stock_code','num'],dtype=(str,int)).set_index('stock_code')
        self.buyin_price=pandas.DataFrame(columns=['stock_code','price'],dtype=(str,float)).set_index('stock_code')
        self.profit=pandas.DataFrame(columns=['stock_code','profit'],dtype=(str,float)).set_index('stock_code')
        return
    
    def estimate_asset(self,date,logger:TradeLogger=None):
        asset=self.money
        if self.stocks.empty==False:
            asset+=(self.stocks['num']*self.buyin_price['price']).sum()
        print("{0}:{1}元".format(date,asset))
        if logger is not None:
            logger.log_asset(date,asset)
        return asset
    
    def sell_all(self,data_manager:DataManager,date:datetime,logger:TradeLogger=None):
        if self.stocks.empty:
            return
        
        price=data_manager.market_data.loc[date].loc[self.stocks.index].close
        success_index=price.index

        self.money+=(price*self.stocks.loc[success_index,'num']).sum()
        profit=(price-self.buyin_price.loc[success_index,'price'])*self.stocks.loc[success_index,'num']
        profit=profit.reindex(self.stocks.index,fill_value=0)
        self.profit=self.profit['profit'].add(profit,fill_value=0).to_frame('profit')

        #T日卖出，T-1交易日买入，记录为T日持仓，盈利记录为T日盈利
        if logger is not None:
            logger.log_holdings(date,self.stocks['num'],profit,self.profit.loc[self.stocks.index,'profit'])

        self.stocks=self.stocks.drop(success_index,axis='index')
        self.buyin_price=self.buyin_price.drop(success_index,axis='index')
        
        if len(self.stocks)!=0:
            print("sell all fail! #remain:{}".format(len(self.stocks)))
        return
    
    def buyin(self,data_manager:DataManager,date:datetime,stocks:pandas.Index):
        if stocks.empty:
            return
        
        avg_money=self.money*min(1/len(stocks),0.05)
        market_data=data_manager.market_data.loc[date].loc[stocks]

        price=market_data.close
        num=(avg_money/price).round(-2).astype(int)
        self.money-=((num*price).sum())
        self.buyin_price=self.buyin_price._append(price.to_frame('price'))
        self.stocks=self.stocks['num'].add(num,fill_value=0).to_frame('num')
        return