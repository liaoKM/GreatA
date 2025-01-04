from Strategy import ActionLogger
import pandas
import matplotlib.pyplot as plt

if __name__=='__main__':
    for year in range(2017,2020):
        actions=pandas.read_csv('./result/actions-{0}.csv'.format(year),dtype={'stock_code':str})
        logger=ActionLogger()
        logger.actions=actions
        avg_info=logger.get_daily_avg_info()
        acc_info=logger.get_total_acc_info()
        avg_info['cum_profit']=avg_info['profit'].cumprod()
        avg_info[['cum_profit','pe']].plot(secondary_y='pe')
        plt.savefig('./result/profit_{0}.png'.format(year))
        plt.clf()
    
