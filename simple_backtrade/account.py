from .data.data_manager import LocalDataManager
class SimpleAccount:
    def __init__(self,init_money):
        self.money=init_money
        self.stocks:dict[str,int]={}
        self.buyin_price:dict[str,int]={}
        self.profit:dict[str,float]={}
        return
    
    def estimate_asset(self,data_manager,date):
        asset=self.money
        market_data=data_manager.get_daily_market_data(date,self.stocks.keys())
        for stock_code,num in self.stocks.items():
            try:
                price=market_data.loc[stock_code].close
            except:
                #停牌
                price=self.buyin_price[stock_code]
            asset+=price*num
        print("{0}:{1}元".format(date,asset))
        return asset
    
    def sell_all(self,data_manager:LocalDataManager,date:str):
        market_data=data_manager.get_daily_market_data(date,self.stocks.keys())
        success_list=[]
        for stock_code,num in self.stocks.items():
            try:
                price=market_data.loc[stock_code].close
                self.money+=price*num
                success_list.append(stock_code)
                profit=(price-self.buyin_price[stock_code])*num
                self.profit[stock_code]=self.profit.get(stock_code,0)+profit
            except:
                #停牌
                pass
        for stock_code in success_list:
            self.stocks.pop(stock_code)
            self.buyin_price.pop(stock_code)
        
        if len(self.stocks)!=0:
            print("clean account fail! #remain:{}".format(len(self.stocks)))
        return
    
    def buyin(self,data_manager:LocalDataManager,date:str,stock_list:list[str]):
        avg_money=self.money/len(stock_list)
        market_data=data_manager.get_daily_market_data(date,stock_list)

        for stock_code in stock_list:
            price=market_data.loc[stock_code].close
            num=round(avg_money/price,-2)
            self.money-=price*num
            self.stocks[stock_code]=self.stocks.get(stock_code,0)+num
            self.buyin_price[stock_code]=price
        
        if self.money<0:
            print("date:{0} money:{1}".format(date,self.money))

        return