import pandas
import numpy as np
import matplotlib.pyplot as plt
from .data.data_manager import DataManager
class TradeLogger:
    def __init__(self):
        # 持仓日志 {日期: {股票代码: 数量}}
        self.holdings_log:pandas.DataFrame = pandas.DataFrame(columns=("stock_code","date","num","daily_profit","total_profit"))\
            .astype({"stock_code":str,"date":str,"num":int,"daily_profit":float,"total_profit":float})\
            .set_index(keys=['stock_code','date'])
        
        # 资产日志 {日期: 总资产}
        self.assets_log = pandas.DataFrame(columns=("date","assets")).astype({"date":str,"assets":float}).set_index('date')
        
        # 收益日志 {日期: 当日收益}
        self.daily_returns = pandas.Series(dtype=np.float64)
        return

    def log_holdings(self,date:str,holdings:pandas.Series,profit:pandas.Series,total_profit:pandas.Series):
        temp_df = pandas.DataFrame({'num': holdings,'daily_profit': profit,'total_profit': total_profit})
        temp_df.index = pandas.MultiIndex.from_tuples([(code, date) for code in temp_df.index],names=['stock_code', 'date'])
        self.holdings_log = pandas.concat([self.holdings_log, temp_df])
        return

    def log_asset(self, date: str, asset: float):
        """记录当日总资产"""
        self.assets_log.at[date,'assets'] = asset
        return

    def prepare_analysis(self, baseline:pandas.Series):
        """预处理分析数据"""
        # 按时间排序
        self.assets_log = self.assets_log.sort_index()
        
        # 计算每日收益率
        self.daily_returns = self.assets_log.pct_change().fillna(0)
        
        # 获取大盘数据（假设大盘代码为'000300.SH'）
        baseline  = baseline.close
        self.benchmark = baseline.pct_change().fillna(0)
        
        #dataframe->Series
        self.assets_log=self.assets_log['assets']
        self.daily_returns=self.daily_returns['assets']

        return

    def analyze(self) -> dict:
        """生成完整分析报告"""
        if self.benchmark is None:
            raise ValueError("请先调用prepare_analysis加载基准数据")

        analysis = {
            'daily_returns': self.daily_returns,
            'benchmark':self.benchmark,
            '年化收益率(%)': self._annualized_return(),
            '日夏普比率': self._sharpe_ratio(),
            '最大回撤(%)': self._max_drawdown(),
            '月胜率(%)': self._monthly_win_rate(),
            '波动率': self._volatility(),
            'Beta': self._beta(),
            'Alpha': self._alpha(),
            #'相对收益(%)': self._relative_return().iloc[-1]*100,
        }

        #save
        start_time=self.daily_returns.index[0].strftime("%Y-%m-%d")
        end_time=self.daily_returns.index[-1].strftime("%Y-%m-%d")
        
        if self.holdings_log.empty==False:
            self.holdings_log.to_csv("./results/{}_{}_holding_log.csv".format(start_time,end_time))
        if self.assets_log.empty==False:
            self.assets_log.to_csv("./results/{}_{}_assets_log.csv".format(start_time,end_time))

        #plot
        plt.rcParams["font.family"] = "Microsoft YaHei"
        plt.figure(figsize=(14, 8))
        cumulative_returns = (1 + self.daily_returns).cumprod()
        cumulative_benchmark = (1 + self.benchmark).cumprod()

        cumulative_returns.plot(label="策略累计收益", color="blue")
        cumulative_benchmark.plot(label="基准累计收益", color="red", linestyle="--")

        # 添加指标文本（使用相对坐标定位）
        text_content = f"""
        年化收益率(%): {analysis['年化收益率(%)']:.2f}
        夏普比率: {analysis['日夏普比率']:.2f}
        最大回撤(%): {analysis['最大回撤(%)']:.2f}
        月胜率(%): {analysis['月胜率(%)']:.2f}
        Beta: {analysis['Beta']:.2f}
        Alpha: {analysis['Alpha']:.2f}"""

        plt.text(
            x=0.72,  # 横向位置（0~1，1为右边界）
            y=0.18,  # 纵向位置（0~1，1为上边界）
            s=text_content,
            transform=plt.gcf().transFigure,  # 使用相对坐标
            fontsize=12,
            verticalalignment="top",  # 文字顶部对齐
            bbox=dict(  # 文本框样式
                boxstyle="round",
                facecolor="white",
                edgecolor="gray",
                alpha=0.8
            )
        )

        # 其他图表设置
        plt.title("策略表现与风险指标", fontsize=14)
        plt.xlabel("日期")
        plt.ylabel("累计收益率")
        plt.legend(loc="upper left")
        plt.grid(linestyle=":", alpha=0.6)

        # 保存图表
        plt.savefig(
            "./results/{}_{}_graph.png".format(start_time,end_time),
            dpi=300,
            bbox_inches="tight"  # 避免文本框被截断
        )
        plt.close()

        return analysis
    
    


    def _cumulative_returns(self) -> pandas.Series:
        return (1 + self.daily_returns).cumprod()

    def _annualized_return(self) -> float:
        days = len(self.daily_returns)
        return (self._cumulative_returns().iloc[-1]**(252/days) - 1) * 100

    def _sharpe_ratio(self, risk_free=0.0) -> float:
        excess_returns = self.daily_returns - risk_free
        return (excess_returns.mean() / excess_returns.std()) * np.sqrt(252)

    def _max_drawdown(self) -> float:
        cum_returns = self._cumulative_returns()
        peak = cum_returns.expanding().max()
        return ((cum_returns - peak)/peak).min() * 100

    def _monthly_win_rate(self) -> float:
        monthly_returns = self.daily_returns.resample('M').apply(lambda x: (1+x).prod()-1)
        benchmark_monthly = self.benchmark.resample('M').apply(lambda x: (1+x).prod()-1)
        return (monthly_returns > benchmark_monthly).mean() * 100

    def _volatility(self) -> float:
        return self.daily_returns.std() * np.sqrt(252) * 100

    def _beta(self) -> float:
        cov = np.cov(self.daily_returns, self.benchmark)[0][1]
        var = self.benchmark.var()
        return cov / var

    def _alpha(self, risk_free=0.0) -> float:
        beta = self._beta()
        annual_ret = self._annualized_return() / 100
        cumulative_return = (self.benchmark + 1).prod()
        bench_annual = cumulative_return**(252 / len(self.benchmark)) - 1
        return (annual_ret - risk_free - beta * (bench_annual - risk_free)) * 100

    def _relative_return(self) -> pandas.Series:
        return self._cumulative_returns() / (1 + self.benchmark).cumprod()

    def get_holdings(self, date: str) -> dict[str, int]:
        """获取指定日期的持仓"""
        return self.holdings_log.get(date, {})