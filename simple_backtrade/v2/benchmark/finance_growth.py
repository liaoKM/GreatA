"""Finance-driven custom index strategies."""

from __future__ import annotations

import pandas as pd

from ..models import TargetPortfolio
from ..strategy.base import BaseStrategy


class FinanceGrowthIndexStrategy(BaseStrategy):
    """Select stocks with strong annual growth and a positive latest report."""

    def __init__(
        self,
        annual_growth_threshold: float = 10.0,
        latest_growth_threshold: float = 0.0,
        annual_report_count: int = 3,
    ):
        self.annual_growth_threshold = float(annual_growth_threshold)
        self.latest_growth_threshold = float(latest_growth_threshold)
        self.annual_report_count = int(annual_report_count)

    def on_bar_close(self, context):
        finance = context.data.finance.asof(context.session_date)
        if finance.empty:
            return TargetPortfolio.empty()

        finance = self._prepare_finance_frame(finance)
        if finance.empty:
            return TargetPortfolio.empty()

        qualified = self._select_constituents(finance)
        return TargetPortfolio.equal_weight(qualified)

    @staticmethod
    def _prepare_finance_frame(finance: pd.DataFrame) -> pd.DataFrame:
        frame = finance.copy()
        frame["end_date"] = pd.to_datetime(frame["end_date"]).dt.normalize()
        frame["ann_date"] = pd.to_datetime(frame["ann_date"]).dt.normalize()
        primary = frame["dt_netprofit_yoy"] if "dt_netprofit_yoy" in frame.columns else pd.Series(index=frame.index, dtype="float64")
        secondary = frame["netprofit_yoy"] if "netprofit_yoy" in frame.columns else pd.Series(index=frame.index, dtype="float64")
        frame["growth"] = pd.to_numeric(primary, errors="coerce")
        fallback = pd.to_numeric(secondary, errors="coerce")
        frame["growth"] = frame["growth"].fillna(fallback)
        frame = frame.dropna(subset=["stock_code", "end_date", "ann_date", "growth"])
        frame["stock_code"] = frame["stock_code"].astype(str)
        frame = frame.sort_values(["stock_code", "end_date", "ann_date", "publish_date", "effective_date"])
        return frame.drop_duplicates(subset=["stock_code", "end_date"], keep="last").reset_index(drop=True)

    def _select_constituents(self, finance: pd.DataFrame) -> list[str]:
        annual_candidates = self._select_annual_candidates(finance)
        if annual_candidates.empty:
            return []

        latest = finance.sort_values(["stock_code", "end_date", "ann_date"]).drop_duplicates(
            subset=["stock_code"],
            keep="last",
        )
        latest = latest[
            latest["stock_code"].isin(annual_candidates["stock_code"])
            & (latest["growth"] > self.latest_growth_threshold)
        ]
        return sorted(latest["stock_code"].astype(str).unique())

    def _select_annual_candidates(self, finance: pd.DataFrame) -> pd.DataFrame:
        annual = finance[finance["end_date"].dt.month.eq(12)].copy()
        if annual.empty:
            return annual.iloc[0:0]

        annual = annual.sort_values(["stock_code", "end_date", "ann_date"]).drop_duplicates(
            subset=["stock_code", "end_date"],
            keep="last",
        )
        annual = annual.groupby("stock_code", group_keys=False).tail(self.annual_report_count)

        qualified = annual.groupby("stock_code").filter(
            lambda group: len(group) >= self.annual_report_count
            and self._has_consecutive_annual_reports(group)
            and bool((group["growth"] > self.annual_growth_threshold).all())
        )
        return qualified.reset_index(drop=True)

    @staticmethod
    def _has_consecutive_annual_reports(group: pd.DataFrame) -> bool:
        years = sorted(group["end_date"].dt.year.astype(int).tolist())
        if len(years) < 2:
            return len(years) == 1
        return all((current - previous) == 1 for previous, current in zip(years, years[1:]))
