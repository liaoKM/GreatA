# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GreatA is a quantitative trading/backtesting system for Chinese A-share stocks. The strategy selects stocks based on financial indicators (ROE, profit growth, leverage ratio) combined with technical indicators (moving averages) for sell signals and market timing.

## Directory Structure

```
e:/GreatA/
├── data/                              # 数据存储目录
│   ├── market/                        # 市场数据
│   │   ├── daily/                     # 日增量 (2025-01-02.parquet)
│   │   ├── monthly/                   # 月汇总 (2025-01.parquet)
│   │   └── yearly/                    # 年汇总 (2025.parquet)
│   ├── finance/                       # 财报数据
│   │   ├── daily/
│   │   ├── monthly/
│   │   └── yearly/
│   ├── announcement/                  # 公告数据
│   │   ├── daily/
│   │   ├── monthly/
│   │   └── yearly/
│   └── meta/                          # 元数据
│       ├── stocks.json                # 股票列表
│       └── last_update.json           # 最后更新日期
│
├── data_service/                      # 数据服务模块（在线）
│   ├── fetcher/                       # 数据拉取
│   │   ├── base.py                    # BaseFetcher 抽象类
│   │   ├── market_fetcher.py          # 市场数据基类（抽象）
│   │   ├── finance_fetcher.py         # 财报数据基类（抽象）
│   │   ├── announcement_fetcher.py    # 公告数据基类（抽象）
│   │   └── tushare/                   # Tushare 数据源实现
│   │       ├── market_fetcher.py      # TushareMarketFetcher
│   │       ├── finance_fetcher.py     # TushareFinanceFetcher
│   │       └── announcement_fetcher.py # TushareAnnouncementFetcher
│   └── aggregator/                    # 数据整合
│       ├── base.py                    # BaseAggregator 抽象类
│       ├── market_aggregator.py
│       ├── finance_aggregator.py
│       └── announcement_aggregator.py
│
├── simple_backtrade/                  # 回测模块（离线，只读数据）
│   ├── backtrade/
│   ├── data/
│   │   ├── manager.py                 # 数据加载
│   │   └── schema.py                  # 字段定义
│   └── strategy/
│
├── scripts/                           # 运行脚本
│   ├── daily_fetch.py                 # 每日拉取
│   ├── aggregate_monthly.py
│   ├── aggregate_yearly.py
│   └── backfill.py                    # 历史回填
│
├── config/
│   └── settings.py
│
└── logs/
```

## Commands

```bash
# 数据拉取
python scripts/daily_fetch.py                    # 拉取今日数据
python scripts/daily_fetch.py --date 2025-01-15  # 拉取指定日期

# 数据整合
python scripts/aggregate_monthly.py              # 整合上月
python scripts/aggregate_monthly.py --year 2025 --month 1
python scripts/aggregate_yearly.py               # 整合去年
python scripts/aggregate_yearly.py --year 2024

# 历史回填
python scripts/backfill.py --start 2024-01-01 --end 2024-12-31
python scripts/backfill.py --type market --start 2024-01-01

# 回测
python run.py                                    # 完整回测 (2014-2025)
python -c "from run import task; task(2024)"     # 单年回测

# 实盘（需要QMT终端）
python xt_run.py
```

## Architecture

### 数据流向

```
tushare API
    │
    ▼
data_service/fetcher/ ──────► data/ (daily)
    │
    ▼
data_service/aggregator/ ───► data/ (monthly/yearly)
    │
    ▼
simple_backtrade/ ◄────────── data/ (只读)
```

### 模块职责

| 模块 | 职责 | 在线/离线 |
|-----|------|----------|
| `data_service/` | 拉取、整合、存储数据 | 在线 |
| `simple_backtrade/` | 回测、策略计算 | 离线 |
| `scripts/` | 触发数据操作 | 手动/定时 |

### Key Components

- **LocalSimulator** (`simple_backtrade/backtrade/simulator.py`): Orchestrates the backtest loop. For each trading day: processes new financial reports, handles dividends/splits, gets target stocks from strategy, executes daily settlement.

- **BaseStrategy** (`simple_backtrade/strategy/base.py`): Abstract base class. Implement `handle_bar(date)` to return stocks to hold, and `handle_report(date, stocks)` to process new financial data.

- **BaseFetcher** (`data_service/fetcher/base.py`): 数据拉取基类。实现 `fetch_daily(date)` 拉取日增量，`fetch_history(start, end)` 拉取历史数据。
- **TushareMarketFetcher** (`data_service/fetcher/tushare/market_fetcher.py`): Tushare市场数据实现，直接调用tushare原生API。

- **BaseAggregator** (`data_service/aggregator/base.py`): 数据整合基类。实现 `aggregate_daily_to_monthly(year, month)` 和 `aggregate_monthly_to_yearly(year)`。

## Strategy Details (MyStrategy)

**Stock Selection Filters:**
- ROE > 10% (annual reports)
- Consecutive non-GAAP profit growth
- Leverage ratio < 70%
- Predicted 2-year PE as ranking metric

**Technical Signals:**
- Sell signal: 20-day moving average
- Cash position: When CSI 300's 60-day MA is declining AND close < 60-day MA

## Data Storage Format

| 数据类型 | 格式 | 索引 | 说明 |
|---------|------|------|------|
| market | Parquet | (date, stock_code) | 日线OHLCV |
| finance | Parquet | (stock_code, report_period) | 财报指标 |
| announcement | Parquet | (ann_date, stock_code) | 公告信息 |

## Output

- `results/*_graph.png` - Performance charts
- `results/*_assets_log.csv` - Daily asset values
- `results/*_holding_log.csv` - Position history
- `latest_socre.csv` - Current stock scores

## Tushare API 文档获取方法

### 文档结构

Tushare Pro API 文档地址: `https://tushare.pro/document/2`

页面使用 jstree 组件展示文档目录结构，核心 HTML 结构为多层 `<ul>/<li>` 嵌套：

```html
<div id="jstree">
    <ul>                                    <!-- depth=1 -->
        <li><a href="?doc_id=14">股票数据</a>  <!-- 第1层节点 -->
            <ul>                            <!-- depth=2 -->
                <li><a href="?doc_id=24">基础数据</a>  <!-- 第2层节点 -->
                    <ul>                    <!-- depth=3 -->
                        <li><a href="?doc_id=25">股票列表</a></li>  <!-- 第3层叶子节点 -->
                        <li><a href="?doc_id=26">交易日历</a></li>
                    </ul>
                </li>
                <li><a href="?doc_id=15">行情数据</a>  <!-- 第2层节点 -->
                    <ul>
                        <li><a href="?doc_id=27">历史日线</a></li>  <!-- 第3层叶子节点 -->
                    </ul>
                </li>
            </ul>
        </li>
    </ul>
</div>
```

**层级示例**:
- 股票数据 [D1] → 基础数据 [D2] → 股票列表 [D3] (3层嵌套)
- ETF专题 [D1] → ETF基本信息 [D2] (2层嵌套，第2层即为叶子节点)

**层级判断规则**: 遍历 HTML 时，遇到 `<ul>` 则深度+1，遇到 `</ul>` 则深度-1，遇到 `<a>` 链接时记录当前深度

### 获取接口文档

**Step 1: 获取文档目录结构**

```python
import re
import urllib.request

url = "https://tushare.pro/document/2"
with urllib.request.urlopen(url, timeout=30) as response:
    content = response.read().decode('utf-8')

# 解析 ul/li 嵌套结构获取所有接口
def get_api_structure(html):
    result = []
    tokens = []

    for m in re.finditer(r'<ul[^>]*>', html):
        tokens.append((m.start(), 'ul_open'))
    for m in re.finditer(r'</ul>', html):
        tokens.append((m.start(), 'ul_close'))
    for m in re.finditer(r'<a[^>]*href="[^"]*doc_id=(\d+)"[^>]*>([^<]+)</a>', html):
        tokens.append((m.start(), 'link', m.group(1), m.group(2)))

    tokens.sort(key=lambda x: x[0])

    depth = 0
    for token in tokens:
        if token[1] == 'ul_open':
            depth += 1
        elif token[1] == 'ul_close':
            depth -= 1
        elif token[1] == 'link':
            result.append({
                'depth': depth,
                'doc_id': token[2],
                'title': token[3].strip()
            })
    return result
```

**Step 2: 获取具体接口参数**

```python
def fetch_api_doc(doc_id):
    """
    获取接口文档详情

    Returns:
        dict: {
            'api': '接口名称（如 income）',
            'desc': '接口描述',
            'tip': '提示内容',
            'input': [...],  # 输入参数表
            'output': [...]  # 输出参数表
        }
    """
    url = f"https://tushare.pro/document/2?doc_id={doc_id}"
    with urllib.request.urlopen(url, timeout=30) as response:
        content = response.read().decode('utf-8')

    def parse_table(html):
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        result = []
        for row in rows:
            cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row, re.DOTALL)
            clean_cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            if clean_cells:
                result.append(clean_cells)
        return result

    result = {'api': '', 'desc': '', 'tip': '', 'input': [], 'output': []}

    # 提取"接口：xxx"（注意冒号是中文）
    api_match = re.search(r'接口[：:]\s*(.*?)(?:<br|</p>|<p|$)', content)
    if api_match:
        text = re.sub(r'<[^>]+>', '', api_match.group(1)).strip()
        result['api'] = text.split('，')[0].split(',')[0].strip()

    # 提取"描述：xxx"
    desc_match = re.search(r'描述[：:]\s*(.*?)(?:<br|</p>|<p|$)', content)
    if desc_match:
        result['desc'] = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()

    # 找到输入参数表的位置
    input_pos = content.find('<p><strong>输入参数</strong></p>')
    if input_pos < 0:
        input_pos = content.find('输入参数')

    # 在输入参数之前查找"提示：xxx"
    if input_pos > 0:
        before_input = content[:input_pos]
        tip_match = re.search(r'提示[：:]\s*(.*?)(?:</font>|</p>|$)', before_input, re.DOTALL)
        if tip_match:
            tip_text = re.sub(r'<[^>]+>', '', tip_match.group(1)).strip()
            result['tip'] = re.sub(r'\s+', ' ', tip_text)

    # 提取输入参数表
    input_match = re.search(r'输入参数.*?<table[^>]*>(.*?)</table>', content, re.DOTALL | re.IGNORECASE)
    if input_match:
        result['input'] = parse_table(input_match.group(1))

    # 提取输出参数表
    output_match = re.search(r'输出参数.*?<table[^>]*>(.*?)</table>', content, re.DOTALL | re.IGNORECASE)
    if output_match:
        result['output'] = parse_table(output_match.group(1))

    return result
```

### 常用接口 doc_id 对照表

| 接口名 | doc_id | 用途 |
|-------|--------|------|
| daily | 27 | 日线行情 |
| index_daily | 95 | 指数日线 |
| trade_cal | 26 | 交易日历 |
| stock_basic | 25 | 股票列表 |
| fina_indicator | 79 | 财务指标 |
| dividend | 103 | 分红送股 |
| adj_factor | 28 | 复权因子 |
| stk_limit | 183 | 涨跌停价格 |

完整接口参考见: `data_service/fetcher/tushare/API_REFERENCE.md`
