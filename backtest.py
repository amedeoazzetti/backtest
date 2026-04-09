"""
Backtesting helpers per Opening Range Breakout (ORB) v1.4.
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
    breakout_minute_stats: pd.DataFrame
    direction_stats: pd.DataFrame
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


def _profit_factor_from_points(result_points: pd.Series) -> float:
    wins = result_points[result_points > 0]
    losses = result_points[result_points <= 0]

    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = abs(float(losses.sum())) if not losses.empty else 0.0

    if gross_loss == 0.0:
        return float("inf") if gross_profit > 0 else 0.0
    return gross_profit / gross_loss


def _compute_performance_snapshot(
    trades: pd.DataFrame,
    initial_capital: float,
    risk_per_trade: float,
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
            "note": "no_trades",
        }
        return metrics, pd.DataFrame(columns=["time", "equity"])

    equity_curve = _build_equity_curve(
        trades=trades,
        initial_capital=initial_capital,
        risk_per_trade=risk_per_trade,
    )

    wins = trades[trades["result_r"] > 0]
    losses = trades[trades["result_r"] <= 0]

    metrics = {
        "total_trades": int(len(trades)),
        "win_rate": float((trades["result_r"] > 0).mean() * 100.0),
        "loss_rate": float((trades["result_r"] <= 0).mean() * 100.0),
        "profit_factor": float(_profit_factor_from_points(trades["result_points"])),
        "average_win": float(wins["result_points"].mean()) if not wins.empty else 0.0,
        "average_loss": float(losses["result_points"].mean()) if not losses.empty else 0.0,
        "expectancy": float(trades["result_points"].mean()),
        "average_r": float(trades["result_r"].mean()),
        "total_r": float(trades["result_r"].sum()),
        "max_drawdown": float(_max_drawdown_pct(equity_curve)),
        "final_equity": float(
            equity_curve["equity"].iloc[-1] if not equity_curve.empty else initial_capital
        ),
        "note": None,
    }
    return metrics, equity_curve


def compute_directional_stats(
    trades: pd.DataFrame,
    initial_capital: float,
    risk_per_trade: float,
) -> pd.DataFrame:
    columns = [
        "side",
        "total_trades",
        "win_rate",
        "profit_factor",
        "average_win",
        "average_loss",
        "expectancy",
        "average_r",
        "total_r",
        "max_drawdown",
        "final_equity",
        "note",
    ]

    if trades.empty:
        return pd.DataFrame(
            [
                {
                    "side": "BOTH",
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "average_win": 0.0,
                    "average_loss": 0.0,
                    "expectancy": 0.0,
                    "average_r": 0.0,
                    "total_r": 0.0,
                    "max_drawdown": 0.0,
                    "final_equity": float(initial_capital),
                    "note": "no_trades",
                },
                {
                    "side": "LONG",
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "average_win": 0.0,
                    "average_loss": 0.0,
                    "expectancy": 0.0,
                    "average_r": 0.0,
                    "total_r": 0.0,
                    "max_drawdown": 0.0,
                    "final_equity": float(initial_capital),
                    "note": "no_trades",
                },
                {
                    "side": "SHORT",
                    "total_trades": 0,
                    "win_rate": 0.0,
                    "profit_factor": 0.0,
                    "average_win": 0.0,
                    "average_loss": 0.0,
                    "expectancy": 0.0,
                    "average_r": 0.0,
                    "total_r": 0.0,
                    "max_drawdown": 0.0,
                    "final_equity": float(initial_capital),
                    "note": "no_trades",
                },
            ],
            columns=columns,
        )

    subsets = {
        "BOTH": trades,
        "LONG": trades[trades["direction"] == "LONG"].copy(),
        "SHORT": trades[trades["direction"] == "SHORT"].copy(),
    }

    rows = []
    for side, subset in subsets.items():
        side_metrics, _ = _compute_performance_snapshot(
            trades=subset,
            initial_capital=initial_capital,
            risk_per_trade=risk_per_trade,
        )
        rows.append(
            {
                "side": side,
                "total_trades": side_metrics["total_trades"],
                "win_rate": side_metrics["win_rate"],
                "profit_factor": side_metrics["profit_factor"],
                "average_win": side_metrics["average_win"],
                "average_loss": side_metrics["average_loss"],
                "expectancy": side_metrics["expectancy"],
                "average_r": side_metrics["average_r"],
                "total_r": side_metrics["total_r"],
                "max_drawdown": side_metrics["max_drawdown"],
                "final_equity": side_metrics["final_equity"],
                "note": side_metrics["note"],
            }
        )

    return pd.DataFrame(rows, columns=columns)


def compute_breakout_minute_stats(trades: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "breakout_minute_bucket",
        "total_trades",
        "win_rate",
        "average_r",
        "total_r",
        "profit_factor",
        "average_result_points",
        "note",
    ]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    df = trades.copy()
    if "breakout_minute_bucket" not in df.columns:
        df["breakout_minute_bucket"] = df["breakout_candle_time"].dt.strftime("%H:%M")

    grouped = (
        df.groupby("breakout_minute_bucket")
        .agg(
            total_trades=("result_r", "size"),
            win_rate=("result_r", lambda x: float((x > 0).mean() * 100.0)),
            average_r=("result_r", "mean"),
            total_r=("result_r", "sum"),
            average_result_points=("result_points", "mean"),
        )
        .reset_index()
    )

    def _bucket_pf(bucket: str) -> float:
        subset = df[df["breakout_minute_bucket"] == bucket]
        return float(_profit_factor_from_points(subset["result_points"]))

    grouped["profit_factor"] = grouped["breakout_minute_bucket"].map(_bucket_pf)
    grouped["note"] = np.where(grouped["total_trades"] < 2, "low_sample", "")

    return grouped[columns].sort_values("breakout_minute_bucket").reset_index(drop=True)


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


def _metric_from_direction(
    direction_stats: pd.DataFrame,
    side: str,
    metric: str,
    default: float = 0.0,
) -> float:
    if direction_stats.empty:
        return default
    row = direction_stats[direction_stats["side"] == side]
    if row.empty:
        return default
    value = row.iloc[0].get(metric, default)
    if pd.isna(value):
        return default
    return float(value)


def _compute_metrics(
    trades: pd.DataFrame,
    initial_capital: float,
    risk_per_trade: float,
    breakout_minute_stats: pd.DataFrame,
    orb_range_stats: pd.DataFrame,
    direction_stats: pd.DataFrame,
) -> tuple[dict, pd.DataFrame]:
    base_metrics, equity_curve = _compute_performance_snapshot(
        trades=trades,
        initial_capital=initial_capital,
        risk_per_trade=risk_per_trade,
    )

    if trades.empty:
        metrics = {
            **base_metrics,
            "tp_hits": 0,
            "sl_hits": 0,
            "time_close_hits": 0,
            "long_trades": 0,
            "short_trades": 0,
            "long_avg_r": 0.0,
            "short_avg_r": 0.0,
            "long_total_r": 0.0,
            "short_total_r": 0.0,
            "trades_by_breakout_window": {},
            "trades_by_breakout_minute": {},
            "breakout_minute_performance": {},
            "breakout_hour_performance": {},
            "orb_range_class_performance": {},
            "directional_performance": _dict_from_stats_table(direction_stats, "side"),
            "note": "no_trades_for_selected_scenario_or_filter",
        }
        return metrics, equity_curve

    df = trades.copy()
    if "breakout_minute_bucket" not in df.columns:
        df["breakout_minute_bucket"] = df["breakout_candle_time"].dt.strftime("%H:%M")

    metrics = {
        **base_metrics,
        "tp_hits": int((df["exit_reason"] == "tp").sum()),
        "sl_hits": int((df["exit_reason"] == "sl").sum()),
        "time_close_hits": int((df["exit_reason"] == "time_close").sum()),
        "long_trades": int((df["direction"] == "LONG").sum()),
        "short_trades": int((df["direction"] == "SHORT").sum()),
        "long_avg_r": _metric_from_direction(direction_stats, "LONG", "average_r", 0.0),
        "short_avg_r": _metric_from_direction(direction_stats, "SHORT", "average_r", 0.0),
        "long_total_r": _metric_from_direction(direction_stats, "LONG", "total_r", 0.0),
        "short_total_r": _metric_from_direction(direction_stats, "SHORT", "total_r", 0.0),
        "trades_by_breakout_window": (
            df["breakout_window_label"].value_counts().sort_index().to_dict()
            if "breakout_window_label" in df.columns
            else {}
        ),
        "trades_by_breakout_minute": df["breakout_minute_bucket"].value_counts().sort_index().to_dict(),
        "breakout_minute_performance": _dict_from_stats_table(
            breakout_minute_stats,
            "breakout_minute_bucket",
        ),
        # Alias mantenuto per retrocompatibilita' v1.2/v1.3
        "breakout_hour_performance": _dict_from_stats_table(
            breakout_minute_stats.rename(columns={"breakout_minute_bucket": "breakout_time_bucket"}),
            "breakout_time_bucket",
        ),
        "orb_range_class_performance": _dict_from_stats_table(
            orb_range_stats,
            "orb_range_class",
        ),
        "directional_performance": _dict_from_stats_table(direction_stats, "side"),
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
    if not trades.empty and "breakout_minute_bucket" not in trades.columns:
        trades["breakout_minute_bucket"] = trades["breakout_candle_time"].dt.strftime("%H:%M")

    trades, range_thresholds = classify_orb_range(trades, orb_range_class_config)
    trades, skipped_by_filter = _apply_orb_range_filter(trades, scenario.allowed_orb_range_classes)

    breakout_minute_stats = compute_breakout_minute_stats(trades)
    breakout_time_stats = breakout_minute_stats.rename(
        columns={"breakout_minute_bucket": "breakout_time_bucket"}
    )
    direction_stats = compute_directional_stats(
        trades=trades,
        initial_capital=initial_capital,
        risk_per_trade=risk_per_trade,
    )
    orb_range_stats = _build_orb_range_stats(trades)

    metrics, equity_curve = _compute_metrics(
        trades=trades,
        initial_capital=initial_capital,
        risk_per_trade=risk_per_trade,
        breakout_minute_stats=breakout_minute_stats,
        orb_range_stats=orb_range_stats,
        direction_stats=direction_stats,
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
        breakout_minute_stats=breakout_minute_stats,
        direction_stats=direction_stats,
        orb_range_stats=orb_range_stats,
        diagnostics=diagnostics,
    )


def format_market_report(result: MarketBacktestResult) -> str:
    m = result.metrics
    lines = [
        "=" * 96,
        f"MARKET: {result.market} | SCENARIO: {result.scenario_label}",
        "=" * 96,
        f"Trades: {m['total_trades']} (LONG: {m['long_trades']} / SHORT: {m['short_trades']})",
        (
            f"Window: {result.breakout_window_label} | Force close: {result.force_close_label} | "
            f"ORB filter: {result.orb_range_filter_label}"
        ),
        f"Win rate: {m['win_rate']:.2f}% | Profit factor: {m['profit_factor']:.4f}",
        f"Average R: {m['average_r']:+.4f} | Total R: {m['total_r']:+.4f}",
        (
            f"Directional R -> LONG avg/total: {m['long_avg_r']:+.4f}/{m['long_total_r']:+.4f} | "
            f"SHORT avg/total: {m['short_avg_r']:+.4f}/{m['short_total_r']:+.4f}"
        ),
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

    minute_distribution = m.get("trades_by_breakout_minute", {})
    if minute_distribution:
        breakdown = ", ".join(f"{key}:{value}" for key, value in minute_distribution.items())
        lines.append(f"Breakout minute distribution: {breakdown}")

    lines.append("=" * 96)
    return "\n".join(lines)


def performance_per_market(results: list[MarketBacktestResult]) -> pd.DataFrame:
    columns = [
        "market",
        "scenario",
        "breakout_window",
        "force_close",
        "orb_range_filter",
        "total_trades",
        "long_trades",
        "short_trades",
        "win_rate",
        "profit_factor",
        "average_r",
        "total_r",
        "long_avg_r",
        "short_avg_r",
        "long_total_r",
        "short_total_r",
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
                "long_trades": metrics["long_trades"],
                "short_trades": metrics["short_trades"],
                "win_rate": metrics["win_rate"],
                "profit_factor": metrics["profit_factor"],
                "average_r": metrics["average_r"],
                "total_r": metrics["total_r"],
                "long_avg_r": metrics["long_avg_r"],
                "short_avg_r": metrics["short_avg_r"],
                "long_total_r": metrics["long_total_r"],
                "short_total_r": metrics["short_total_r"],
                "max_drawdown": metrics["max_drawdown"],
                "final_equity": metrics["final_equity"],
            }
        )

    return (
        pd.DataFrame(rows)
        .sort_values(["market", "breakout_window", "force_close", "orb_range_filter"])
        .reset_index(drop=True)
    )
