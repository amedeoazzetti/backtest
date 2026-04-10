"""
Data utilities for ORB backtesting.

Primary workflow:
- load 5m data (CSV or external provider)
- normalize to clean OHLCV in America/New_York
- resample to 15m for ORB candle detection
- validate 15m block completeness (3x5m per 15m candle)
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
        parsed_time = _parse_datetime_series(working[source_time_col])
        keep_mask = ~parsed_time.isna()
        dropped_bad_datetime_rows = int((~keep_mask).sum())

        working = working.loc[keep_mask].copy()
        parsed_time = parsed_time.loc[keep_mask]
        working = working.drop(columns=[source_time_col])
        working.index = pd.DatetimeIndex(parsed_time, name="time")
    else:
        if not isinstance(working.index, pd.DatetimeIndex):
            raise ValueError("Il DataFrame deve avere DatetimeIndex oppure la colonna Time.")

        parsed_index = pd.to_datetime(working.index, errors="coerce")
        keep_mask = ~parsed_index.isna()
        dropped_bad_datetime_rows = int((~keep_mask).sum())
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
    working = working.loc[~bad_ohlc_mask].copy()

    volume_nan_rows = int(working["volume"].isna().sum())
    if volume_nan_rows:
        working["volume"] = working["volume"].fillna(0.0)

    bad_high_low_mask = working["high"] < working["low"]
    dropped_bad_high_low_rows = int(bad_high_low_mask.sum())
    working = working.loc[~bad_high_low_mask].copy()

    working = working.sort_index()
    duplicated_before_tz = int(working.index.duplicated(keep="last").sum())
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

    duplicated_after_tz = int(working.index.duplicated(keep="last").sum())
    if duplicated_after_tz:
        working = working[~working.index.duplicated(keep="last")].copy()

    if working.empty:
        raise ValueError("Dataset vuoto dopo la pulizia dei dati 5m.")

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
    }
    return working, diagnostics


def resample_to_15m(df_5m: pd.DataFrame) -> pd.DataFrame:
    """Resample a cleaned 5m dataframe to 15m OHLCV with source bar count."""
    if df_5m.empty:
        raise ValueError("Impossibile resamplare: dataset 5m vuoto.")

    missing = sorted(set(OHLCV_COLUMNS).difference(df_5m.columns))
    if missing:
        raise ValueError(f"Impossibile resamplare a 15m, colonne mancanti: {', '.join(missing)}")

    m15 = (
        df_5m.resample("15min", label="left", closed="left")
        .agg(
            open=("open", "first"),
            high=("high", "max"),
            low=("low", "min"),
            close=("close", "last"),
            volume=("volume", "sum"),
            source_5m_count=("close", "count"),
        )
        .dropna(subset=OHLC_COLUMNS)
    )

    m15["source_5m_count"] = pd.to_numeric(m15["source_5m_count"], errors="coerce").fillna(0).astype(int)
    m15["is_complete_block"] = m15["source_5m_count"] == 3
    return m15


def validate_15m_blocks(
    df_15m: pd.DataFrame,
    orb_start: time = time(9, 30),
) -> dict[str, Any]:
    """Validate completeness and alignment of 15m candles."""
    if df_15m.empty:
        return {
            "m15_total_bars": 0,
            "m15_incomplete_bars": 0,
            "m15_complete_bars": 0,
            "m15_misaligned_bars": 0,
            "m15_orb_0930_bars": 0,
            "m15_orb_0930_incomplete_bars": 0,
        }

    if "source_5m_count" not in df_15m.columns:
        raise ValueError("validate_15m_blocks richiede la colonna source_5m_count.")

    counts = pd.to_numeric(df_15m["source_5m_count"], errors="coerce").fillna(0)
    incomplete_mask = counts < 3
    misaligned_mask = (df_15m.index.minute % 15) != 0
    orb_mask = pd.Index(df_15m.index.time) == orb_start

    return {
        "m15_total_bars": int(len(df_15m)),
        "m15_incomplete_bars": int(incomplete_mask.sum()),
        "m15_complete_bars": int((~incomplete_mask).sum()),
        "m15_misaligned_bars": int(misaligned_mask.sum()),
        "m15_orb_0930_bars": int(orb_mask.sum()),
        "m15_orb_0930_incomplete_bars": int((incomplete_mask & orb_mask).sum()),
    }


def _build_5m_and_15m(
    raw_df: pd.DataFrame,
    source_timezone: str,
    source_label: str,
    time_column: Optional[str],
    require_volume: bool,
    drop_incomplete_15m_blocks: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    clean_5m, clean_diag = normalize_5m_dataframe(
        raw_df=raw_df,
        source_timezone=source_timezone,
        target_timezone=TARGET_TIMEZONE,
        time_column=time_column,
        require_volume=require_volume,
    )

    m15_full = resample_to_15m(clean_5m)
    m15_diag = validate_15m_blocks(m15_full)

    incomplete_policy = "drop" if drop_incomplete_15m_blocks else "keep"
    dropped_incomplete_bars = 0
    if drop_incomplete_15m_blocks:
        incomplete_mask = ~m15_full["is_complete_block"]
        dropped_incomplete_bars = int(incomplete_mask.sum())
        m15_ready = m15_full.loc[~incomplete_mask].copy()
    else:
        m15_ready = m15_full.copy()

    if m15_ready.empty:
        raise ValueError("Nessuna candela 15m valida dopo il controllo completezza blocchi.")

    diagnostics = {
        "data_source": source_label,
        **clean_diag,
        **m15_diag,
        "m15_incomplete_policy": incomplete_policy,
        "m15_dropped_incomplete_bars": dropped_incomplete_bars,
        "m15_ready_bars": int(len(m15_ready)),
    }
    return clean_5m, m15_ready, diagnostics


def load_csv_market_data(
    csv_path: str | Path,
    csv_timezone: str,
    drop_incomplete_15m_blocks: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Load CSV 5m data and derive clean 5m + validated 15m datasets."""
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

    clean_5m, m15_ready, diagnostics = _build_5m_and_15m(
        raw_df=raw_df,
        source_timezone=csv_timezone,
        source_label="csv",
        time_column="Time",
        require_volume=True,
        drop_incomplete_15m_blocks=drop_incomplete_15m_blocks,
    )

    diagnostics["csv_path"] = str(path)
    diagnostics["csv_rows_read"] = int(len(raw_df))
    return clean_5m, m15_ready, diagnostics


def prepare_external_market_data(
    raw_df: pd.DataFrame,
    source_timezone_fallback: str = "UTC",
    drop_incomplete_15m_blocks: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Normalize provider 5m data and derive validated 15m bars."""
    clean_5m, m15_ready, diagnostics = _build_5m_and_15m(
        raw_df=raw_df,
        source_timezone=source_timezone_fallback,
        source_label="provider",
        time_column=None,
        require_volume=False,
        drop_incomplete_15m_blocks=drop_incomplete_15m_blocks,
    )

    diagnostics["provider_rows_read"] = int(len(raw_df))
    return clean_5m, m15_ready, diagnostics
