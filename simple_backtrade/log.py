import pandas
import numpy as np
from data.data_manager import LocalDataManager
class TradeLogger:
    def __init__(self,data_manager:LocalDataManager):
        self.data_manager=data_manager

        # 持仓日志 {日期: {股票代码: 数量}}
        self.holdings_log = pandas.DataFrame(columns=("stock_code","date","num","daily_profit","total_profit"),dtype=(str,str,int,float,float)).set_index('stock_code','date')
        
        # 资产日志 {日期: 总资产}
        self.assets_log = pandas.DataFrame(columns=("date","assets"),dtype=(str,float)).set_index('date')
        
        # 收益日志 {日期: 当日收益}
        self.daily_returns = pandas.Series(dtype=np.float64)
        
        # 大盘基准数据缓存
        self.benchmark = pandas.DataFrame(columns=("date","000300.SH"),dtype=(str,float)).set_index('date')
        return

    def log_holdings(self, date: str, holdings: Dict[str, int]):
        """记录持仓信息（第二天生效的持仓）"""
        self.holdings_log[date] = holdings.copy()

    def log_asset(self, date: str, asset: float):
        """记录当日总资产"""
        self.assets_log[date] = asset

    def prepare_analysis(self, data_manager):
        """预处理分析数据"""
        # 按时间排序
        self.assets_log = self.assets_log.sort_index()
        
        # 计算每日收益率
        self.daily_returns = self.assets_log.pct_change().fillna(0)
        
        # 获取大盘数据（假设大盘代码为'000300.SH'）
        benchmark_data = data_manager.get_index_data('000300.SH')
        self.benchmark = benchmark_data['close'].pct_change().fillna(0)

    def analyze(self) -> Dict:
        """生成完整分析报告"""
        if self.benchmark is None:
            raise ValueError("请先调用prepare_analysis加载基准数据")

        analysis = {
            '累计收益': self._cumulative_returns(),
            '年化收益率': self._annualized_return(),
            '夏普比率': self._sharpe_ratio(),
            '最大回撤': self._max_drawdown(),
            '月胜率': self._monthly_win_rate(),
            '波动率': self._volatility(),
            'Beta': self._beta(),
            'Alpha': self._alpha(),
            '相对收益': self._relative_return(),
        }
        return analysis

    def _cumulative_returns(self) -> pd.Series:
        return (1 + self.daily_returns).cumprod()

    def _annualized_return(self) -> float:
        days = len(self.daily_returns)
        return (self._cumulative_returns().iloc[-1] ​** (252/days) - 1) * 100

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
        cov = np.cov(self.daily_returns, self.benchmark[:len(self.daily_returns)])[0][1]
        var = self.benchmark.var()
        return cov / var

    def _alpha(self, risk_free=0.0) -> float:
        beta = self._beta()
        annual_ret = self._annualized_return() / 100
        bench_annual = (self.benchmark.mean() * 252) * 100
        return (annual_ret - risk_free - beta * (bench_annual - risk_free)) * 100

    def _relative_return(self) -> pd.Series:
        return self._cumulative_returns() / (1 + self.benchmark).cumprod()

    def get_holdings(self, date: str) -> Dict[str, int]:
        """获取指定日期的持仓"""
        return self.holdings_log.get(date, {})