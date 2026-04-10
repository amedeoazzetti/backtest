"""
Audit helpers for ORB data pipeline and strategy diagnostics (M5-only architecture).
"""

from __future__ import annotations

from datetime import date, datetime, time
import json
from pathlib import Path
from typing import Any

import pandas as pd

from data_utils import build_daily_orb_from_m5


MISSING_ORB_COLUMNS = [
    "market",
    "trade_date",
    "reason",
    "bars_0930_0945_count",
    "has_0930_5m",
    "has_0935_5m",
    "has_0940_5m",
    "duplicate_orb_time_rows",
]

ORB_RESAMPLE_AUDIT_COLUMNS = [
    "market",
    "trade_date",
    "source_5m_time_1",
    "source_5m_time_2",
    "source_5m_time_3",
    "source_5m_open_1",
    "source_5m_high_1",
    "source_5m_low_1",
    "source_5m_close_1",
    "source_5m_volume_1",
    "source_5m_open_2",
    "source_5m_high_2",
    "source_5m_low_2",
    "source_5m_close_2",
    "source_5m_volume_2",
    "source_5m_open_3",
    "source_5m_high_3",
    "source_5m_low_3",
    "source_5m_close_3",
    "source_5m_volume_3",
    "resampled_15m_time",
    "resampled_15m_open",
    "resampled_15m_high",
    "resampled_15m_low",
    "resampled_15m_close",
    "resampled_15m_volume",
    "expected_orb_bar_present",
    "source_triplet_complete",
    "resample_consistency_ok",
    "consistency_issue",
]

AMBIGUOUS_AUDIT_COLUMNS = [
    "market",
    "scenario",
    "trade_date",
    "candle_time_ny",
    "direction_candidate",
    "orb_high",
    "orb_low",
    "candle_open",
    "candle_high",
    "candle_low",
    "candle_close",
    "ambiguity_reason",
    "how_strategy_handled_it",
]

TRADE_REPLAY_AUDIT_COLUMNS = [
    "market",
    "scenario",
    "trade_date",
    "direction",
    "breakout_candle_time",
    "entry_time",
    "entry_price",
    "stop_loss",
    "take_profit",
    "exit_time",
    "exit_reason",
    "orb_high",
    "orb_low",
    "orb_range_class",
    "rr_target",
    "trade_direction_mode",
    "breakout_candle_open",
    "breakout_candle_high",
    "breakout_candle_low",
    "breakout_candle_close",
    "entry_candle_open",
    "entry_candle_high",
    "entry_candle_low",
    "entry_candle_close",
    "exit_candle_open",
    "exit_candle_high",
    "exit_candle_low",
    "exit_candle_close",
]


def parse_audit_dates(raw_dates: str | None) -> set[date]:
    if raw_dates is None or not raw_dates.strip():
        return set()

    parsed: set[date] = set()
    for token in raw_dates.split(","):
        item = token.strip()
        if not item:
            continue
        try:
            parsed_date = datetime.strptime(item, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                f"Data audit non valida: {item}. Formato atteso: YYYY-MM-DD"
            ) from exc
        parsed.add(parsed_date)

    return parsed


def select_audit_dates(
    df_5m: pd.DataFrame,
    sample_days: int,
    requested_dates: set[date],
) -> tuple[list[date], list[str]]:
    if sample_days <= 0:
        raise ValueError("--audit-sample-days deve essere > 0")

    available_dates = sorted({ts.date() for ts in pd.DatetimeIndex(df_5m.index)})
    if not available_dates:
        return [], []

    if requested_dates:
        available_set = set(available_dates)
        selected = sorted(d for d in requested_dates if d in available_set)
        missing = sorted(d.isoformat() for d in requested_dates if d not in available_set)
        return selected, missing

    return available_dates[:sample_days], []


def _format_ts(ts: pd.Timestamp | None) -> str | None:
    if ts is None or pd.isna(ts):
        return None
    return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S %Z")


def _series_value(row: pd.Series | None, col: str) -> float | None:
    if row is None or col not in row or pd.isna(row[col]):
        return None
    return float(row[col])


def _extract_5m_bar(day_5m: pd.DataFrame, bar_time: time) -> tuple[pd.Timestamp | None, pd.Series | None]:
    rows = day_5m[day_5m.index.time == bar_time]
    if rows.empty:
        return None, None
    idx = rows.index[0]
    return pd.Timestamp(idx), rows.iloc[0]


def audit_missing_orb_days(
    df_5m: pd.DataFrame,
    market_label: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    day_groups = [(d, frame) for d, frame in df_5m.groupby(df_5m.index.date)]
    day_groups.sort(key=lambda x: x[0])

    for trade_date, day_5m in day_groups:
        orb_info = build_daily_orb_from_m5(day_5m)
        if orb_info.get("is_valid", False):
            continue

        rows.append(
            {
                "market": market_label,
                "trade_date": trade_date.isoformat(),
                "reason": str(orb_info.get("reason") or "unknown"),
                "bars_0930_0945_count": int(orb_info.get("bars_0930_0945_count", 0)),
                "has_0930_5m": bool(orb_info.get("has_0930_5m", False)),
                "has_0935_5m": bool(orb_info.get("has_0935_5m", False)),
                "has_0940_5m": bool(orb_info.get("has_0940_5m", False)),
                "duplicate_orb_time_rows": int(orb_info.get("duplicate_orb_time_rows", 0)),
            }
        )

    return pd.DataFrame(rows, columns=MISSING_ORB_COLUMNS)


def audit_orb_resampling(
    df_5m: pd.DataFrame,
    market_label: str,
    selected_dates: list[date],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for trade_date in selected_dates:
        day_5m = df_5m[df_5m.index.date == trade_date]
        orb_info = build_daily_orb_from_m5(day_5m)

        t1, b1 = _extract_5m_bar(day_5m, time(9, 30))
        t2, b2 = _extract_5m_bar(day_5m, time(9, 35))
        t3, b3 = _extract_5m_bar(day_5m, time(9, 40))

        source_triplet_complete = all(x is not None for x in [b1, b2, b3])
        expected_orb_bar_present = bool(orb_info.get("is_valid", False))

        consistency_ok = False
        consistency_issue = str(orb_info.get("reason") or "")

        calc_open = None
        calc_high = None
        calc_low = None
        calc_close = None
        calc_volume = None

        if source_triplet_complete:
            calc_open = float(b1["open"])
            calc_high = float(max(b1["high"], b2["high"], b3["high"]))
            calc_low = float(min(b1["low"], b2["low"], b3["low"]))
            calc_close = float(b3["close"])
            calc_volume = float(b1.get("volume", 0.0) + b2.get("volume", 0.0) + b3.get("volume", 0.0))

            if expected_orb_bar_present:
                checks = {
                    "open": abs(calc_open - float(orb_info["orb_open"])) < 1e-9,
                    "high": abs(calc_high - float(orb_info["orb_high"])) < 1e-9,
                    "low": abs(calc_low - float(orb_info["orb_low"])) < 1e-9,
                    "close": abs(calc_close - float(orb_info["orb_close"])) < 1e-9,
                    "volume": abs(calc_volume - float(orb_info["orb_volume"])) < 1e-9,
                }
                consistency_ok = all(checks.values())
                if consistency_ok:
                    consistency_issue = ""
                else:
                    consistency_issue = ",".join(key for key, ok in checks.items() if not ok)
            else:
                consistency_ok = False
                if not consistency_issue:
                    consistency_issue = "invalid_or_missing_orb_triplet"
        else:
            consistency_ok = False
            if not consistency_issue:
                consistency_issue = "incomplete_source_triplet"

        rows.append(
            {
                "market": market_label,
                "trade_date": trade_date.isoformat(),
                "source_5m_time_1": _format_ts(t1),
                "source_5m_time_2": _format_ts(t2),
                "source_5m_time_3": _format_ts(t3),
                "source_5m_open_1": _series_value(b1, "open"),
                "source_5m_high_1": _series_value(b1, "high"),
                "source_5m_low_1": _series_value(b1, "low"),
                "source_5m_close_1": _series_value(b1, "close"),
                "source_5m_volume_1": _series_value(b1, "volume"),
                "source_5m_open_2": _series_value(b2, "open"),
                "source_5m_high_2": _series_value(b2, "high"),
                "source_5m_low_2": _series_value(b2, "low"),
                "source_5m_close_2": _series_value(b2, "close"),
                "source_5m_volume_2": _series_value(b2, "volume"),
                "source_5m_open_3": _series_value(b3, "open"),
                "source_5m_high_3": _series_value(b3, "high"),
                "source_5m_low_3": _series_value(b3, "low"),
                "source_5m_close_3": _series_value(b3, "close"),
                "source_5m_volume_3": _series_value(b3, "volume"),
                "resampled_15m_time": _format_ts(t1),
                "resampled_15m_open": float(orb_info["orb_open"]) if expected_orb_bar_present else calc_open,
                "resampled_15m_high": float(orb_info["orb_high"]) if expected_orb_bar_present else calc_high,
                "resampled_15m_low": float(orb_info["orb_low"]) if expected_orb_bar_present else calc_low,
                "resampled_15m_close": float(orb_info["orb_close"]) if expected_orb_bar_present else calc_close,
                "resampled_15m_volume": float(orb_info["orb_volume"]) if expected_orb_bar_present else calc_volume,
                "expected_orb_bar_present": bool(expected_orb_bar_present),
                "source_triplet_complete": bool(source_triplet_complete),
                "resample_consistency_ok": bool(consistency_ok),
                "consistency_issue": consistency_issue,
            }
        )

    return pd.DataFrame(rows, columns=ORB_RESAMPLE_AUDIT_COLUMNS)


def build_dataset_audit_summary(
    market_label: str,
    df_5m: pd.DataFrame,
    diagnostics: dict[str, Any],
    missing_orb_days: pd.DataFrame,
    selected_audit_dates: list[date],
    audit_dates_out_of_range: list[str],
) -> dict[str, Any]:
    all_dates = sorted({ts.date() for ts in pd.DatetimeIndex(df_5m.index)})
    weekday_dates = [d for d in all_dates if d.weekday() < 5]

    missing_set = set(
        pd.to_datetime(missing_orb_days.get("trade_date", pd.Series(dtype=str)), errors="coerce")
        .dropna()
        .dt.date
        .tolist()
    )

    valid_orb_dates = [d for d in all_dates if d not in missing_set]
    valid_orb_weekdays = [d for d in weekday_dates if d not in missing_set]

    reason_counts: dict[str, int] = {}
    if not missing_orb_days.empty and "reason" in missing_orb_days.columns:
        reason_counts = {str(k): int(v) for k, v in missing_orb_days["reason"].value_counts().to_dict().items()}

    summary = {
        "market": market_label,
        "source": diagnostics.get("data_source"),
        "source_timezone": diagnostics.get("source_timezone"),
        "target_timezone": diagnostics.get("target_timezone"),
        "source_first_timestamp_raw": diagnostics.get("source_first_timestamp_raw"),
        "source_last_timestamp_raw": diagnostics.get("source_last_timestamp_raw"),
        "source_first_timestamp_parsed": diagnostics.get("source_first_timestamp_parsed"),
        "source_last_timestamp_parsed": diagnostics.get("source_last_timestamp_parsed"),
        "target_first_timestamp_converted": diagnostics.get("target_first_timestamp_converted"),
        "target_last_timestamp_converted": diagnostics.get("target_last_timestamp_converted"),
        "rows_input": diagnostics.get("rows_input"),
        "rows_after_cleaning": diagnostics.get("rows_after_cleaning"),
        "total_dates": len(all_dates),
        "weekday_dates": len(weekday_dates),
        "dates_with_valid_orb": len(valid_orb_dates),
        "weekday_dates_with_valid_orb": len(valid_orb_weekdays),
        "dates_without_orb": int(len(all_dates) - len(valid_orb_dates)),
        "weekday_dates_without_orb": int(len(weekday_dates) - len(valid_orb_weekdays)),
        "dates_with_duplicate_or_anomaly_rows": len(diagnostics.get("dates_with_anomalies", [])),
        "bars_5m_at_0930": int((pd.DatetimeIndex(df_5m.index).time == time(9, 30)).sum()),
        "bars_5m_at_0935": int((pd.DatetimeIndex(df_5m.index).time == time(9, 35)).sum()),
        "bars_5m_at_0940": int((pd.DatetimeIndex(df_5m.index).time == time(9, 40)).sum()),
        "missing_orb_days_count": int(len(missing_orb_days)),
        "missing_orb_reasons": reason_counts,
        "selected_audit_dates": [d.isoformat() for d in selected_audit_dates],
        "audit_dates_out_of_range": audit_dates_out_of_range,
    }
    return summary


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = None
    return out[columns]


def save_audit_outputs(
    audit_dir: Path,
    dataset_summary: dict[str, Any],
    missing_orb_days: pd.DataFrame,
    ambiguous_signal_candles: pd.DataFrame,
    orb_resample_audit: pd.DataFrame,
    trade_replay_audit: pd.DataFrame,
) -> dict[str, Path]:
    audit_dir.mkdir(parents=True, exist_ok=True)

    paths = {
        "dataset_audit_summary": audit_dir / "dataset_audit_summary.json",
        "missing_orb_days": audit_dir / "missing_orb_days.csv",
        "ambiguous_signal_candles": audit_dir / "ambiguous_signal_candles.csv",
        "orb_resample_audit": audit_dir / "orb_resample_audit.csv",
        "trade_replay_audit": audit_dir / "trade_replay_audit.csv",
    }

    with paths["dataset_audit_summary"].open("w", encoding="utf-8") as fp:
        json.dump(dataset_summary, fp, indent=2, ensure_ascii=True)

    _ensure_columns(missing_orb_days, MISSING_ORB_COLUMNS).to_csv(paths["missing_orb_days"], index=False)
    _ensure_columns(ambiguous_signal_candles, AMBIGUOUS_AUDIT_COLUMNS).to_csv(
        paths["ambiguous_signal_candles"],
        index=False,
    )
    _ensure_columns(orb_resample_audit, ORB_RESAMPLE_AUDIT_COLUMNS).to_csv(
        paths["orb_resample_audit"],
        index=False,
    )
    _ensure_columns(trade_replay_audit, TRADE_REPLAY_AUDIT_COLUMNS).to_csv(
        paths["trade_replay_audit"],
        index=False,
    )

    return paths
