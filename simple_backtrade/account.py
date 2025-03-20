class SimpleAccount:
    def __init__(self,init_money):
        self.money=init_money
        self.stocks:dict[str,int]={}
        return