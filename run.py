import matplotlib.pyplot as plt


import simple_backtrade

if __name__=='__main__':
    # simple_backtrade.adata_request.update(2025,2025)
    
    profit_dataframe_list=[]
    start_time='2018-01-01'
    end_time='2018-05-31'

    simulator=simple_backtrade.backtrade.LocalSimulator(start_time,end_time)
    simulator.start()
    
    

    pass