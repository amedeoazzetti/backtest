from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from backtest import MarketBacktestResult
from config import market_slug


PRIMARY_FORCE_CLOSE_LABELS = {"no_time_close", "time_close_1200"}


def _json_default_serializer(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def _prepare_trade_export(trades: pd.DataFrame) -> pd.DataFrame:
    out = trades.copy()
    for col in ["breakout_candle_time", "entry_time", "exit_time"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").astype(str)
    return out


def save_scenario_outputs(result: MarketBacktestResult, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    m_slug = market_slug(result.market)
    s_slug = result.scenario_label

    paths = {
        "trades": output_dir / f"trades_{m_slug}_{s_slug}.csv",
        "metrics": output_dir / f"metrics_{m_slug}_{s_slug}.json",
        "equity": output_dir / f"equity_{m_slug}_{s_slug}.csv",
        "breakout_time_stats": output_dir / f"breakout_time_stats_{m_slug}_{s_slug}.csv",
        "orb_range_stats": output_dir / f"orb_range_stats_{m_slug}_{s_slug}.csv",
    }

    _prepare_trade_export(result.trades).to_csv(paths["trades"], index=False)
    result.equity_curve.to_csv(paths["equity"], index=False)
    result.breakout_time_stats.to_csv(paths["breakout_time_stats"], index=False)
    result.orb_range_stats.to_csv(paths["orb_range_stats"], index=False)

    with paths["metrics"].open("w", encoding="utf-8") as fp:
        json.dump(result.metrics, fp, indent=2, ensure_ascii=True, default=_json_default_serializer)

    return paths


def summarize_outputs(paths: dict[str, Path]) -> str:
    ordered_keys = [
        "trades",
        "metrics",
        "equity",
        "breakout_time_stats",
        "orb_range_stats",
    ]
    lines = ["File salvati:"]
    for key in ordered_keys:
        path = paths[key]
        lines.append(f"  - {path.name}")
    return "\n".join(lines)


def split_primary_secondary(table: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if table.empty or "force_close" not in table.columns:
        return table.copy(), pd.DataFrame(columns=table.columns)

    primary = table[table["force_close"].isin(PRIMARY_FORCE_CLOSE_LABELS)].copy()
    secondary = table[~table["force_close"].isin(PRIMARY_FORCE_CLOSE_LABELS)].copy()
    return primary.reset_index(drop=True), secondary.reset_index(drop=True)
