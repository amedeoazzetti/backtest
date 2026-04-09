"""
Backtesting helpers per Opening Range Breakout (ORB) v1.2.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from config import ORBRangeClassConfig, ORBRangeClassResult, ScenarioConfig
from strategy import ORBConfig, OpeningRangeBreakoutStrategy


@dataclass(frozen=True)
class MarketBacktestResult:
    market: str
    scenario_label: str
    force_close_label: str
    breakout_window_label: str
    orb_range_filter_label: str
    trades: pd.DataFrame
    metrics: dict
    equity_curve: pd.DataFrame
    breakout_time_stats: pd.DataFrame
    orb_range_stats: pd.DataFrame
    diagnostics: dict


def _ensure_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ["breakout_candle_time", "entry_time", "exit_time"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce")
    return out


def _build_equity_curve(
    trades: pd.DataFrame,
    initial_capital: float,
    risk_per_trade: float,
) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["time", "equity"])

    equity = float(initial_capital)
    rows = []
    for _, trade in trades.sort_values("exit_time").iterrows():
        equity *= 1.0 + (float(trade["result_r"]) * risk_per_trade)
        rows.append({"time": trade["exit_time"], "equity": equity})

    return pd.DataFrame(rows)


def _max_drawdown_pct(equity_curve: pd.DataFrame) -> float:
    if equity_curve.empty:
        return 0.0

    values = equity_curve["equity"].astype(float)
    peaks = values.cummax()
    drawdowns = (values - peaks) / peaks * 100.0
    return float(drawdowns.min())


def classify_orb_range(
    trades: pd.DataFrame,
    range_config: ORBRangeClassConfig,
) -> tuple[pd.DataFrame, ORBRangeClassResult]:
    """Classifica ORB range in small/medium/large usando quantili su orb_range_pct_of_entry."""
    if trades.empty:
        return trades.copy(), ORBRangeClassResult(
            lower=None,
            upper=None,
            method="quantile_pct_of_entry",
            quantiles=(range_config.lower_quantile, range_config.upper_quantile),
        )

    out = trades.copy()
    metric_col = "orb_range_pct_of_entry"
    metric_values = pd.to_numeric(out[metric_col], errors="coerce").replace([np.inf, -np.inf], np.nan)

    valid = metric_values.dropna()
    if len(valid) < 3:
        out["orb_range_class"] = "medium"
        return out, ORBRangeClassResult(
            lower=None,
            upper=None,
            method="fallback_medium",
            quantiles=(range_config.lower_quantile, range_config.upper_quantile),
        )

    lower_thr = float(valid.quantile(range_config.lower_quantile))
    upper_thr = float(valid.quantile(range_config.upper_quantile))

    if lower_thr >= upper_thr:
        out["orb_range_class"] = "medium"
        return out, ORBRangeClassResult(
            lower=lower_thr,
            upper=upper_thr,
            method="fallback_medium_equal_threshold",
            quantiles=(range_config.lower_quantile, range_config.upper_quantile),
        )

    out["orb_range_class"] = np.select(
        [metric_values < lower_thr, metric_values > upper_thr],
        ["small", "large"],
        default="medium",
    )

    return out, ORBRangeClassResult(
        lower=lower_thr,
        upper=upper_thr,
        method="quantile_pct_of_entry",
        quantiles=(range_config.lower_quantile, range_config.upper_quantile),
    )


def _apply_orb_range_filter(
    trades: pd.DataFrame,
    allowed_classes: frozenset[str] | None,
) -> tuple[pd.DataFrame, int]:
    if trades.empty or allowed_classes is None:
        return trades.copy(), 0

    mask = trades["orb_range_class"].isin(allowed_classes)
    filtered = trades.loc[mask].copy()
    skipped = int((~mask).sum())
    return filtered, skipped


def _build_breakout_time_stats(trades: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "breakout_time_bucket",
        "total_trades",
        "win_rate",
        "average_r",
        "total_r",
        "average_result_points",
    ]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    df = trades.copy()
    df["breakout_time_bucket"] = df["breakout_candle_time"].dt.strftime("%H:%M")

    grouped = (
        df.groupby("breakout_time_bucket")
        .agg(
            total_trades=("result_r", "size"),
            win_rate=("result_r", lambda x: float((x > 0).mean() * 100.0)),
            average_r=("result_r", "mean"),
            total_r=("result_r", "sum"),
            average_result_points=("result_points", "mean"),
        )
        .reset_index()
    )
    return grouped.sort_values("breakout_time_bucket").reset_index(drop=True)


def _build_orb_range_stats(trades: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "orb_range_class",
        "total_trades",
        "win_rate",
        "average_r",
        "total_r",
        "average_result_points",
        "average_orb_range_points",
        "average_orb_range_pct_of_entry",
    ]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    grouped = (
        trades.groupby("orb_range_class")
        .agg(
            total_trades=("result_r", "size"),
            win_rate=("result_r", lambda x: float((x > 0).mean() * 100.0)),
            average_r=("result_r", "mean"),
            total_r=("result_r", "sum"),
            average_result_points=("result_points", "mean"),
            average_orb_range_points=("orb_range_points", "mean"),
            average_orb_range_pct_of_entry=("orb_range_pct_of_entry", "mean"),
        )
        .reset_index()
    )

    sort_order = {"small": 0, "medium": 1, "large": 2, "unclassified": 3}
    grouped["_order"] = grouped["orb_range_class"].map(sort_order).fillna(99)
    grouped = grouped.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
    return grouped


def _dict_from_stats_table(df: pd.DataFrame, key_col: str) -> dict:
    if df.empty:
        return {}
    return df.set_index(key_col).round(6).to_dict(orient="index")


def _compute_metrics(
    trades: pd.DataFrame,
    initial_capital: float,
    risk_per_trade: float,
    breakout_time_stats: pd.DataFrame,
    orb_range_stats: pd.DataFrame,
) -> tuple[dict, pd.DataFrame]:
    if trades.empty:
        metrics = {
            "total_trades": 0,
            "win_rate": 0.0,
            "loss_rate": 0.0,
            "profit_factor": 0.0,
            "average_win": 0.0,
            "average_loss": 0.0,
            "expectancy": 0.0,
            "average_r": 0.0,
            "total_r": 0.0,
            "max_drawdown": 0.0,
            "final_equity": float(initial_capital),
            "tp_hits": 0,
            "sl_hits": 0,
            "time_close_hits": 0,
            "long_trades": 0,
            "short_trades": 0,
            "trades_by_breakout_window": {},
            "trades_by_breakout_hour": {},
            "breakout_hour_performance": {},
            "orb_range_class_performance": {},
            "note": "no_trades_for_selected_scenario_or_filter",
        }
        return metrics, pd.DataFrame(columns=["time", "equity"])

    trades = _ensure_datetime_columns(trades)
    wins = trades[trades["result_r"] > 0]
    losses = trades[trades["result_r"] <= 0]

    gross_profit = float(wins["result_points"].sum())
    gross_loss = abs(float(losses["result_points"].sum()))
    if gross_loss == 0:
        profit_factor = float("inf") if gross_profit > 0 else 0.0
    else:
        profit_factor = gross_profit / gross_loss

    equity_curve = _build_equity_curve(
        trades=trades,
        initial_capital=initial_capital,
        risk_per_trade=risk_per_trade,
    )

    breakout_hour_counts = {}
    if not trades.empty:
        breakout_hour_counts = (
            trades["breakout_candle_time"].dt.strftime("%H:%M").value_counts().sort_index().to_dict()
        )

    metrics = {
        "total_trades": int(len(trades)),
        "win_rate": float((trades["result_r"] > 0).mean() * 100.0),
        "loss_rate": float((trades["result_r"] <= 0).mean() * 100.0),
        "profit_factor": float(profit_factor),
        "average_win": float(wins["result_points"].mean()) if not wins.empty else 0.0,
        "average_loss": float(losses["result_points"].mean()) if not losses.empty else 0.0,
        "expectancy": float(trades["result_points"].mean()),
        "average_r": float(trades["result_r"].mean()),
        "total_r": float(trades["result_r"].sum()),
        "max_drawdown": float(_max_drawdown_pct(equity_curve)),
        "final_equity": float(
            equity_curve["equity"].iloc[-1] if not equity_curve.empty else initial_capital
        ),
        "tp_hits": int((trades["exit_reason"] == "tp").sum()),
        "sl_hits": int((trades["exit_reason"] == "sl").sum()),
        "time_close_hits": int((trades["exit_reason"] == "time_close").sum()),
        "long_trades": int((trades["direction"] == "LONG").sum()),
        "short_trades": int((trades["direction"] == "SHORT").sum()),
        "trades_by_breakout_window": (
            trades["breakout_window_label"].value_counts().sort_index().to_dict()
            if "breakout_window_label" in trades.columns
            else {}
        ),
        "trades_by_breakout_hour": breakout_hour_counts,
        "breakout_hour_performance": _dict_from_stats_table(
            breakout_time_stats,
            "breakout_time_bucket",
        ),
        "orb_range_class_performance": _dict_from_stats_table(
            orb_range_stats,
            "orb_range_class",
        ),
        "note": None,
    }

    return metrics, equity_curve


def run_market_backtest(
    df: pd.DataFrame,
    scenario: ScenarioConfig,
    max_trades_per_day: int,
    initial_capital: float,
    risk_per_trade: float,
    orb_range_class_config: ORBRangeClassConfig,
) -> MarketBacktestResult:
    strategy_config = ORBConfig(
        market=scenario.market_label,
        signal_end=scenario.breakout_window_end,
        breakout_window_label=scenario.breakout_window_label,
        force_close_time=scenario.force_close_time,
        max_trades_per_day=max_trades_per_day,
    )

    strategy = OpeningRangeBreakoutStrategy(config=strategy_config)
    trades, diagnostics = strategy.run(df)

    trades = _ensure_datetime_columns(trades)
    trades, range_thresholds = classify_orb_range(trades, orb_range_class_config)
    trades, skipped_by_filter = _apply_orb_range_filter(trades, scenario.allowed_orb_range_classes)

    breakout_time_stats = _build_breakout_time_stats(trades)
    orb_range_stats = _build_orb_range_stats(trades)

    metrics, equity_curve = _compute_metrics(
        trades=trades,
        initial_capital=initial_capital,
        risk_per_trade=risk_per_trade,
        breakout_time_stats=breakout_time_stats,
        orb_range_stats=orb_range_stats,
    )

    metrics["market"] = scenario.market_label
    metrics["scenario"] = scenario.scenario_label
    metrics["force_close"] = scenario.force_close_label
    metrics["breakout_window"] = scenario.breakout_window_label
    metrics["orb_range_filter"] = scenario.orb_range_filter_label
    metrics["orb_range_class_thresholds"] = {
        "lower": range_thresholds.lower,
        "upper": range_thresholds.upper,
        "method": range_thresholds.method,
        "quantiles": list(range_thresholds.quantiles) if range_thresholds.quantiles else None,
    }
    metrics["filtered_out_trades_by_orb_filter"] = skipped_by_filter

    diagnostics = {
        **diagnostics,
        "scenario": scenario.scenario_label,
        "orb_range_filter": scenario.orb_range_filter_label,
        "filtered_out_trades_by_orb_filter": skipped_by_filter,
    }

    return MarketBacktestResult(
        market=scenario.market_label,
        scenario_label=scenario.scenario_label,
        force_close_label=scenario.force_close_label,
        breakout_window_label=scenario.breakout_window_label,
        orb_range_filter_label=scenario.orb_range_filter_label,
        trades=trades,
        metrics=metrics,
        equity_curve=equity_curve,
        breakout_time_stats=breakout_time_stats,
        orb_range_stats=orb_range_stats,
        diagnostics=diagnostics,
    )


def format_market_report(result: MarketBacktestResult) -> str:
    m = result.metrics
    lines = [
        "=" * 90,
        f"MARKET: {result.market} | SCENARIO: {result.scenario_label}",
        "=" * 90,
        f"Trades: {m['total_trades']} (LONG: {m['long_trades']} / SHORT: {m['short_trades']})",
        (
            f"Window: {result.breakout_window_label} | Force close: {result.force_close_label} | "
            f"ORB filter: {result.orb_range_filter_label}"
        ),
        f"Exit reason -> TP: {m['tp_hits']} | SL: {m['sl_hits']} | TIME_CLOSE: {m['time_close_hits']}",
        f"Win rate: {m['win_rate']:.2f}% | Loss rate: {m['loss_rate']:.2f}%",
        f"Profit factor: {m['profit_factor']:.4f}",
        f"Average win (points): {m['average_win']:+.4f}",
        f"Average loss (points): {m['average_loss']:+.4f}",
        f"Expectancy (points): {m['expectancy']:+.4f}",
        f"Average R: {m['average_r']:+.4f} | Total R: {m['total_r']:+.4f}",
        f"Max drawdown: {m['max_drawdown']:.2f}% | Final equity: {m['final_equity']:.2f}",
        (
            "Diagnostics -> "
            f"ambiguous_signal_candles: {result.diagnostics.get('ambiguous_signal_candles', 0)}, "
            f"invalid_risk_entries: {result.diagnostics.get('invalid_risk_entries', 0)}, "
            f"days_missing_orb: {result.diagnostics.get('days_missing_orb', 0)}, "
            f"rows_dropped_nan: {result.diagnostics.get('rows_dropped_nan', 0)}, "
            f"filtered_out_by_orb_filter: {result.diagnostics.get('filtered_out_trades_by_orb_filter', 0)}"
        ),
    ]

    if m.get("trades_by_breakout_hour"):
        breakdown = ", ".join(
            f"{key}:{value}" for key, value in m["trades_by_breakout_hour"].items()
        )
        lines.append(f"Breakout hour distribution: {breakdown}")

    if m.get("orb_range_class_performance"):
        class_breakdown = ", ".join(
            f"{key}:{int(value['total_trades'])}" for key, value in m["orb_range_class_performance"].items()
        )
        lines.append(f"ORB range class distribution: {class_breakdown}")

    lines.append("=" * 90)
    return "\n".join(lines)


def performance_per_market(results: list[MarketBacktestResult]) -> pd.DataFrame:
    columns = [
        "market",
        "scenario",
        "breakout_window",
        "force_close",
        "orb_range_filter",
        "total_trades",
        "win_rate",
        "profit_factor",
        "average_r",
        "total_r",
        "max_drawdown",
        "final_equity",
    ]
    if not results:
        return pd.DataFrame(columns=columns)

    rows = []
    for result in results:
        metrics = result.metrics
        rows.append(
            {
                "market": result.market,
                "scenario": result.scenario_label,
                "breakout_window": result.breakout_window_label,
                "force_close": result.force_close_label,
                "orb_range_filter": result.orb_range_filter_label,
                "total_trades": metrics["total_trades"],
                "win_rate": metrics["win_rate"],
                "profit_factor": metrics["profit_factor"],
                "average_r": metrics["average_r"],
                "total_r": metrics["total_r"],
                "max_drawdown": metrics["max_drawdown"],
                "final_equity": metrics["final_equity"],
            }
        )

    return (
        pd.DataFrame(rows)
        .sort_values(["market", "breakout_window", "force_close", "orb_range_filter"])
        .reset_index(drop=True)
    )
