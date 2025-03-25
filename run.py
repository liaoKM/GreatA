import matplotlib.pyplot as plt


import simple_backtrade

if __name__=='__main__':
    #simple_backtrade.adata_request.update(2025,2025)
    
    start_time='2025-01-01'
    end_time='2025-12-31'

    simulator=simple_backtrade.backtrade.LocalSimulator(start_time,end_time)
    simulator.start()
    
    

    pass