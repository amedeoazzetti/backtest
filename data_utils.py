"""
Data utilities for ORB backtesting (M5-only architecture).

Primary workflow:
- load and clean M5 data (CSV or external provider)
- convert timestamps to America/New_York
- build ORB from the exact 09:30 / 09:35 / 09:40 M5 triplet

Legacy note:
- the old 15m-resample path is no longer the primary engine path.
"""

from __future__ import annotations

from datetime import time
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pandas as pd


TARGET_TIMEZONE = "America/New_York"
OHLC_COLUMNS = ["open", "high", "low", "close"]
OHLCV_COLUMNS = ["open", "high", "low", "close", "volume"]
ORB_REQUIRED_TIMES = (time(9, 30), time(9, 35), time(9, 40))


def _resolve_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Timezone non valido: {name}") from exc


def _parse_datetime_series(values: pd.Series) -> pd.Series:
    text = values.astype(str).str.strip()
    parsed = pd.to_datetime(text, format="%m/%d/%Y %H:%M", errors="coerce")

    missing_mask = parsed.isna()
    if missing_mask.any():
        fallback = pd.to_datetime(text[missing_mask], errors="coerce")
        parsed.loc[missing_mask] = fallback

    return parsed


def _date_strings_from_index(index_values: pd.Index) -> set[str]:
    idx = pd.to_datetime(index_values, errors="coerce")
    out: set[str] = set()
    for ts in idx:
        if pd.isna(ts):
            continue
        out.add(pd.Timestamp(ts).date().isoformat())
    return out


def normalize_5m_dataframe(
    raw_df: pd.DataFrame,
    source_timezone: str,
    target_timezone: str = TARGET_TIMEZONE,
    time_column: Optional[str] = None,
    require_volume: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize raw 5m OHLCV data to a clean NY-time indexed DataFrame."""
    if raw_df.empty:
        raise ValueError("Dataset vuoto: nessuna riga da normalizzare.")

    source_tz = _resolve_timezone(source_timezone)
    target_tz = _resolve_timezone(target_timezone)

    working = raw_df.copy()
    rows_input = int(len(working))
    anomaly_dates: set[str] = set()

    source_first_timestamp_raw: Optional[str] = None
    source_last_timestamp_raw: Optional[str] = None
    source_first_timestamp_parsed: Optional[str] = None
    source_last_timestamp_parsed: Optional[str] = None

    dropped_bad_datetime_rows = 0
    if time_column is not None:
        column_lookup = {str(c).strip().lower(): c for c in working.columns}
        requested = time_column.strip().lower()
        if requested not in column_lookup:
            available = ", ".join(str(c) for c in working.columns)
            raise ValueError(
                f"Colonna datetime non trovata: {time_column}. Colonne disponibili: {available}"
            )

        source_time_col = column_lookup[requested]
        source_time_text = working[source_time_col].astype(str).str.strip()
        parsed_time = _parse_datetime_series(source_time_text)
        keep_mask = ~parsed_time.isna()
        dropped_bad_datetime_rows = int((~keep_mask).sum())

        valid_source_text = source_time_text.loc[keep_mask]
        valid_parsed_time = parsed_time.loc[keep_mask]
        if not valid_parsed_time.empty:
            source_first_timestamp_raw = str(valid_source_text.iloc[0])
            source_last_timestamp_raw = str(valid_source_text.iloc[-1])
            source_first_timestamp_parsed = pd.Timestamp(valid_parsed_time.iloc[0]).isoformat()
            source_last_timestamp_parsed = pd.Timestamp(valid_parsed_time.iloc[-1]).isoformat()

        working = working.loc[keep_mask].copy()
        parsed_time = parsed_time.loc[keep_mask]
        working = working.drop(columns=[source_time_col])
        working.index = pd.DatetimeIndex(parsed_time, name="time")
    else:
        if not isinstance(working.index, pd.DatetimeIndex):
            raise ValueError("Il DataFrame deve avere DatetimeIndex oppure la colonna Time.")

        raw_index_values = pd.Index(working.index)
        parsed_index = pd.to_datetime(raw_index_values, errors="coerce")
        keep_mask = ~parsed_index.isna()
        dropped_bad_datetime_rows = int((~keep_mask).sum())

        valid_raw_index = raw_index_values[keep_mask]
        valid_parsed_index = parsed_index[keep_mask]
        if len(valid_parsed_index) > 0:
            source_first_timestamp_raw = str(valid_raw_index[0])
            source_last_timestamp_raw = str(valid_raw_index[-1])
            source_first_timestamp_parsed = pd.Timestamp(valid_parsed_index[0]).isoformat()
            source_last_timestamp_parsed = pd.Timestamp(valid_parsed_index[-1]).isoformat()

        working = working.loc[keep_mask].copy()
        parsed_index = parsed_index[keep_mask]
        working.index = pd.DatetimeIndex(parsed_index, name="time")

    working.columns = [str(c).strip().lower() for c in working.columns]

    required = set(OHLC_COLUMNS)
    if require_volume:
        required.add("volume")

    missing_cols = sorted(required.difference(working.columns))
    if missing_cols:
        raise ValueError(f"Colonne mancanti: {', '.join(missing_cols)}")

    if "volume" not in working.columns:
        working["volume"] = 0.0

    for col in OHLCV_COLUMNS:
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    bad_ohlc_mask = working[OHLC_COLUMNS].isna().any(axis=1)
    dropped_bad_ohlc_rows = int(bad_ohlc_mask.sum())
    if dropped_bad_ohlc_rows:
        anomaly_dates.update(_date_strings_from_index(working.index[bad_ohlc_mask]))
    working = working.loc[~bad_ohlc_mask].copy()

    volume_nan_rows = int(working["volume"].isna().sum())
    if volume_nan_rows:
        working["volume"] = working["volume"].fillna(0.0)

    bad_high_low_mask = working["high"] < working["low"]
    dropped_bad_high_low_rows = int(bad_high_low_mask.sum())
    if dropped_bad_high_low_rows:
        anomaly_dates.update(_date_strings_from_index(working.index[bad_high_low_mask]))
    working = working.loc[~bad_high_low_mask].copy()

    working = working.sort_index()
    duplicate_mask_before_tz = working.index.duplicated(keep="last")
    duplicated_before_tz = int(duplicate_mask_before_tz.sum())
    if duplicated_before_tz:
        anomaly_dates.update(_date_strings_from_index(working.index[duplicate_mask_before_tz]))
    if duplicated_before_tz:
        working = working[~working.index.duplicated(keep="last")].copy()

    timestamps_were_tz_aware = working.index.tz is not None
    dropped_tz_ambiguous_rows = 0
    if not timestamps_were_tz_aware:
        localized = working.index.tz_localize(
            source_tz,
            ambiguous="NaT",
            nonexistent="shift_forward",
        )
        keep_after_localize = ~pd.isna(localized)
        dropped_tz_ambiguous_rows = int((~keep_after_localize).sum())
        if dropped_tz_ambiguous_rows:
            working = working.iloc[keep_after_localize].copy()
            localized = localized[keep_after_localize]
        working.index = localized

    working.index = working.index.tz_convert(target_tz)
    working = working.sort_index()

    duplicate_mask_after_tz = working.index.duplicated(keep="last")
    duplicated_after_tz = int(duplicate_mask_after_tz.sum())
    if duplicated_after_tz:
        anomaly_dates.update(_date_strings_from_index(working.index[duplicate_mask_after_tz]))
    if duplicated_after_tz:
        working = working[~working.index.duplicated(keep="last")].copy()

    if working.empty:
        raise ValueError("Dataset vuoto dopo la pulizia dei dati 5m.")

    target_first_timestamp_converted = pd.Timestamp(working.index[0]).isoformat()
    target_last_timestamp_converted = pd.Timestamp(working.index[-1]).isoformat()

    diagnostics = {
        "rows_input": rows_input,
        "rows_after_cleaning": int(len(working)),
        "dropped_bad_datetime_rows": dropped_bad_datetime_rows,
        "dropped_bad_ohlc_rows": dropped_bad_ohlc_rows,
        "dropped_bad_high_low_rows": dropped_bad_high_low_rows,
        "dropped_duplicate_rows": duplicated_before_tz + duplicated_after_tz,
        "filled_missing_volume_rows": volume_nan_rows,
        "dropped_tz_ambiguous_rows": dropped_tz_ambiguous_rows,
        "timestamps_were_tz_aware": bool(timestamps_were_tz_aware),
        "source_timezone": source_timezone,
        "target_timezone": target_timezone,
        "source_first_timestamp_raw": source_first_timestamp_raw,
        "source_last_timestamp_raw": source_last_timestamp_raw,
        "source_first_timestamp_parsed": source_first_timestamp_parsed,
        "source_last_timestamp_parsed": source_last_timestamp_parsed,
        "target_first_timestamp_converted": target_first_timestamp_converted,
        "target_last_timestamp_converted": target_last_timestamp_converted,
        "dates_with_anomalies": sorted(anomaly_dates),
    }
    return working, diagnostics


def _build_clean_m5(
    raw_df: pd.DataFrame,
    source_timezone: str,
    source_label: str,
    time_column: Optional[str],
    require_volume: bool,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    clean_5m, clean_diag = normalize_5m_dataframe(
        raw_df=raw_df,
        source_timezone=source_timezone,
        target_timezone=TARGET_TIMEZONE,
        time_column=time_column,
        require_volume=require_volume,
    )

    diagnostics = {
        "data_source": source_label,
        **clean_diag,
    }
    return clean_5m, diagnostics


def _single_bar_for_time(day_5m: pd.DataFrame, bar_time: time) -> tuple[pd.Timestamp | None, pd.Series | None, int]:
    bars = day_5m[day_5m.index.time == bar_time]
    if bars.empty:
        return None, None, 0
    if len(bars) > 1:
        return pd.Timestamp(bars.index[0]), bars.iloc[0], len(bars)
    return pd.Timestamp(bars.index[0]), bars.iloc[0], 1


def validate_orb_triplet(
    day_5m: pd.DataFrame,
    orb_start: time = time(9, 30),
) -> dict[str, Any]:
    """Validate and build ORB candle directly from 09:30/09:35/09:40 M5 bars."""
    result: dict[str, Any] = {
        "is_valid": False,
        "reason": "unknown",
        "bars_0930_0945_count": 0,
        "has_0930_5m": False,
        "has_0935_5m": False,
        "has_0940_5m": False,
        "duplicate_orb_time_rows": 0,
        "source_0930_time": None,
        "source_0935_time": None,
        "source_0940_time": None,
        "orb_open": None,
        "orb_high": None,
        "orb_low": None,
        "orb_close": None,
        "orb_volume": None,
    }

    if day_5m is None or day_5m.empty:
        result["reason"] = "all_5m_missing"
        return result

    orb_end = time(9, 45)
    window = day_5m[(day_5m.index.time >= orb_start) & (day_5m.index.time < orb_end)]
    result["bars_0930_0945_count"] = int(len(window))

    if window.empty:
        result["reason"] = "session_missing"
        return result

    t930, b930, dup930 = _single_bar_for_time(day_5m, time(9, 30))
    t935, b935, dup935 = _single_bar_for_time(day_5m, time(9, 35))
    t940, b940, dup940 = _single_bar_for_time(day_5m, time(9, 40))

    result["has_0930_5m"] = b930 is not None
    result["has_0935_5m"] = b935 is not None
    result["has_0940_5m"] = b940 is not None
    result["duplicate_orb_time_rows"] = int(max(dup930 - 1, 0) + max(dup935 - 1, 0) + max(dup940 - 1, 0))
    result["source_0930_time"] = t930
    result["source_0935_time"] = t935
    result["source_0940_time"] = t940

    if result["duplicate_orb_time_rows"] > 0:
        result["reason"] = "duplicate_orb_bar"
        return result

    source_complete = bool(b930 is not None and b935 is not None and b940 is not None)
    if not source_complete:
        expected_minutes = {30, 35, 40}
        observed_minutes = {int(ts.minute) for ts in pd.DatetimeIndex(window.index)}
        if observed_minutes and observed_minutes.difference(expected_minutes):
            result["reason"] = "timezone_alignment_issue"
        else:
            result["reason"] = "incomplete_0930_block"
        return result

    if int(len(window)) != 3:
        result["reason"] = "incomplete_0930_block"
        return result

    orb_open = float(b930["open"])
    orb_high = float(max(b930["high"], b935["high"], b940["high"]))
    orb_low = float(min(b930["low"], b935["low"], b940["low"]))
    orb_close = float(b940["close"])
    orb_volume = float(b930.get("volume", 0.0) + b935.get("volume", 0.0) + b940.get("volume", 0.0))

    if orb_high < orb_low:
        result["reason"] = "invalid_orb_range"
        return result

    result["is_valid"] = True
    result["reason"] = ""
    result["orb_open"] = orb_open
    result["orb_high"] = orb_high
    result["orb_low"] = orb_low
    result["orb_close"] = orb_close
    result["orb_volume"] = orb_volume
    return result


def build_daily_orb_from_m5(day_5m: pd.DataFrame) -> dict[str, Any]:
    """Build daily ORB directly from required M5 triplet."""
    return validate_orb_triplet(day_5m=day_5m, orb_start=time(9, 30))


def load_m5_csv(
    csv_path: str | Path,
    csv_timezone: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load CSV and return cleaned M5 dataset in America/New_York."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"File CSV non trovato: {path}")
    if not path.is_file():
        raise ValueError(f"Percorso CSV non valido (non e un file): {path}")

    try:
        raw_df = pd.read_csv(path, on_bad_lines="skip")
    except pd.errors.EmptyDataError as exc:
        raise ValueError(f"CSV vuoto: {path}") from exc

    if raw_df.empty:
        raise ValueError(f"CSV vuoto o senza righe valide: {path}")

    clean_5m, diagnostics = _build_clean_m5(
        raw_df=raw_df,
        source_timezone=csv_timezone,
        source_label="csv",
        time_column="Time",
        require_volume=True,
    )

    diagnostics["csv_path"] = str(path)
    diagnostics["csv_rows_read"] = int(len(raw_df))
    return clean_5m, diagnostics


def load_csv_market_data(
    csv_path: str | Path,
    csv_timezone: str,
    drop_incomplete_15m_blocks: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compatibility wrapper: use load_m5_csv in M5-only architecture."""
    _ = drop_incomplete_15m_blocks
    return load_m5_csv(csv_path=csv_path, csv_timezone=csv_timezone)


def prepare_external_market_data(
    raw_df: pd.DataFrame,
    source_timezone_fallback: str = "UTC",
    drop_incomplete_15m_blocks: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Normalize provider data and return cleaned M5 dataset in America/New_York."""
    _ = drop_incomplete_15m_blocks
    clean_5m, diagnostics = _build_clean_m5(
        raw_df=raw_df,
        source_timezone=source_timezone_fallback,
        source_label="provider",
        time_column=None,
        require_volume=False,
    )

    diagnostics["provider_rows_read"] = int(len(raw_df))
    return clean_5m, diagnostics
