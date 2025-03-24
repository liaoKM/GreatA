import matplotlib.pyplot as plt


import simple_backtrade

if __name__=='__main__':
    simple_backtrade.adata_request.update(2024,2024)
    
    start_time='2021-01-01'
    end_time='2021-12-31'

    simulator=simple_backtrade.backtrade.LocalSimulator(start_time,end_time)
    simulator.start()
    
    

    pass