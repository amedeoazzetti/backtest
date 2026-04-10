"""
CLI principale per backtest ORB (fase M5-only rigorosa).

Esempi:
    python main.py --markets SP500,NASDAQ --breakout-windows 10:00,10:30 --orb-range-filters all,small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both
    python main.py --csv-path-sp500 data/sp500_5m.csv --csv-path-nasdaq data/nasdaq_5m.csv --csv-timezone UTC --markets SP500,NASDAQ
"""

from __future__ import annotations

import argparse
from datetime import datetime
import sys
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("Installa yfinance: pip install yfinance")
    sys.exit(1)

from audit_utils import (
    AMBIGUOUS_AUDIT_COLUMNS,
    ORB_RESAMPLE_AUDIT_COLUMNS,
    TRADE_REPLAY_AUDIT_COLUMNS,
    build_dataset_audit_summary,
    audit_missing_orb_days,
    audit_orb_resampling,
    parse_audit_dates,
    save_audit_outputs,
    select_audit_dates,
)
from backtest import format_market_report, performance_per_market, run_market_backtest
from config import (
    DEFAULT_BREAKOUT_WINDOWS,
    DEFAULT_FORCE_CLOSE_OPTIONS,
    DEFAULT_MARKETS,
    DEFAULT_ORB_RANGE_FILTERS,
    DEFAULT_RR_TARGETS,
    DEFAULT_TRADE_DIRECTION_MODES,
    MARKET_LABELS,
    build_market_scenarios,
    market_slug,
    parse_breakout_windows,
    parse_force_close_options,
    parse_markets,
    parse_orb_range_filters,
    parse_orb_range_quantiles,
    parse_rr_targets,
    parse_trade_direction_modes,
)
from data_utils import load_m5_csv, prepare_external_market_data
from reporting import save_scenario_outputs, split_primary_secondary, summarize_outputs


def fetch_provider_data(symbol: str, period: str, interval: str) -> pd.DataFrame:
    print(f"Scarico {symbol} | period={period} | interval={interval}")
    df = yf.download(
        tickers=symbol,
        period=period,
        interval=interval,
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if df.empty:
        raise ValueError(f"Nessun dato ricevuto per {symbol}")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    return df


def _resolve_market_csv_path(args: argparse.Namespace, market_code: str) -> str | None:
    if market_code == "SP500" and args.csv_path_sp500:
        return str(args.csv_path_sp500)
    if market_code == "NASDAQ" and args.csv_path_nasdaq:
        return str(args.csv_path_nasdaq)
    if args.csv_path:
        return str(args.csv_path)
    return None


def _print_data_diagnostics(
    market_label: str,
    diagnostics: dict[str, Any],
    total_dates: int,
    valid_orb_dates: int,
    missing_orb_dates: int,
) -> None:
    print(f"\nData diagnostics [{market_label}] ({diagnostics.get('data_source', 'unknown')}):")

    rows_read = diagnostics.get("csv_rows_read", diagnostics.get("provider_rows_read", 0))
    print(
        "  rows: "
        f"letto={rows_read}, "
        f"pulito_5m={diagnostics.get('rows_after_cleaning', 0)}"
    )
    print(
        "  timezone: "
        f"sorgente={diagnostics.get('source_timezone', 'n/a')} -> "
        f"finale={diagnostics.get('target_timezone', 'n/a')}"
    )
    print(
        "  timezone_trace: "
        f"first_raw={diagnostics.get('source_first_timestamp_raw')}, "
        f"first_ny={diagnostics.get('target_first_timestamp_converted')}, "
        f"last_raw={diagnostics.get('source_last_timestamp_raw')}, "
        f"last_ny={diagnostics.get('target_last_timestamp_converted')}"
    )
    print(
        "  pulizia_5m: "
        f"bad_datetime={diagnostics.get('dropped_bad_datetime_rows', 0)}, "
        f"bad_ohlc={diagnostics.get('dropped_bad_ohlc_rows', 0)}, "
        f"high_lt_low={diagnostics.get('dropped_bad_high_low_rows', 0)}, "
        f"duplicati={diagnostics.get('dropped_duplicate_rows', 0)}"
    )
    print(
        "  orb_daily_check: "
        f"date_totali={total_dates}, "
        f"orb_valide={valid_orb_dates}, "
        f"orb_mancanti={missing_orb_dates}"
    )


def _empty_missing_orb_frame(market_label: str) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "market",
            "trade_date",
            "reason",
            "bars_0930_0945_count",
            "has_0930_5m",
            "has_0935_5m",
            "has_0940_5m",
            "duplicate_orb_time_rows",
        ]
    ).assign(market=market_label)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest ORB M5-only")
    parser.add_argument("--csv-path", default=None, help="CSV M5 fallback per tutti i mercati")
    parser.add_argument("--csv-path-sp500", default=None, help="CSV M5 dedicato per SP500")
    parser.add_argument("--csv-path-nasdaq", default=None, help="CSV M5 dedicato per NASDAQ")
    parser.add_argument(
        "--csv-timezone",
        default="UTC",
        help="Timezone dei timestamp CSV (es: UTC, America/New_York)",
    )
    parser.add_argument(
        "--m15-incomplete-policy",
        choices=["drop", "keep"],
        default="drop",
        help="Legacy option (ignorata in modalita M5-only).",
    )
    parser.add_argument(
        "--audit-mode",
        action="store_true",
        help="Abilita output audit/debug del pipeline dati e della logica ORB",
    )
    parser.add_argument(
        "--audit-sample-days",
        type=int,
        default=10,
        help="Numero giorni campione per audit ORB triplet (default: 10)",
    )
    parser.add_argument(
        "--audit-dates",
        default=None,
        help="Date esplicite per audit ORB (YYYY-MM-DD,YYYY-MM-DD)",
    )
    parser.add_argument(
        "--audit-trades-limit",
        type=int,
        default=20,
        help="Numero massimo trade per scenario nel trade replay audit (default: 20)",
    )
    parser.add_argument("--period", default="120d", help="Periodo storico provider (default: 120d)")
    parser.add_argument("--interval", default="5m", help="Timeframe dati provider (default: 5m)")
    parser.add_argument(
        "--markets",
        default=DEFAULT_MARKETS,
        help=f"Mercati separati da virgola (default: {DEFAULT_MARKETS})",
    )
    parser.add_argument(
        "--force-close-options",
        default=DEFAULT_FORCE_CLOSE_OPTIONS,
        help=f"Scenari force-close, default: {DEFAULT_FORCE_CLOSE_OPTIONS}",
    )
    parser.add_argument(
        "--breakout-windows",
        default=DEFAULT_BREAKOUT_WINDOWS,
        help=f"Fine finestra breakout separata da virgola, default: {DEFAULT_BREAKOUT_WINDOWS}",
    )
    parser.add_argument(
        "--orb-range-filters",
        default=DEFAULT_ORB_RANGE_FILTERS,
        help=(
            "Filtri classe ORB range separati da virgola. "
            "Valori: all, small, medium, large, small+medium, medium+large, small+large"
        ),
    )
    parser.add_argument(
        "--orb-range-quantiles",
        default="0.33,0.66",
        help="Quantili classe ORB range (small/medium/large), esempio: 0.33,0.66",
    )
    parser.add_argument(
        "--rr-targets",
        default=DEFAULT_RR_TARGETS,
        help="Multipli RR separati da virgola, esempio: 1.0,1.5,2.0,3.0",
    )
    parser.add_argument(
        "--trade-direction-modes",
        default=DEFAULT_TRADE_DIRECTION_MODES,
        help="Modalita direzione trade: both,long_only,short_only",
    )
    parser.add_argument("--max-trades-per-day", type=int, default=2, help="Max trade giornalieri (default: 2)")
    parser.add_argument("--capital", type=float, default=10000.0, help="Capitale iniziale equity model")
    parser.add_argument(
        "--risk-per-trade",
        type=float,
        default=0.01,
        help="Rischio per trade nell'equity model (default: 0.01)",
    )
    parser.add_argument("--output-dir", default="outputs", help="Cartella output")
    args = parser.parse_args()

    if args.max_trades_per_day <= 0:
        print("Errore parametri: --max-trades-per-day deve essere > 0")
        sys.exit(1)

    if not (0 < args.risk_per_trade < 1):
        print("Errore parametri: --risk-per-trade deve essere tra 0 e 1")
        sys.exit(1)

    if args.audit_sample_days <= 0:
        print("Errore parametri: --audit-sample-days deve essere > 0")
        sys.exit(1)

    if args.audit_trades_limit <= 0:
        print("Errore parametri: --audit-trades-limit deve essere > 0")
        sys.exit(1)

    if args.m15_incomplete_policy != "drop":
        print("Nota: --m15-incomplete-policy e legacy e viene ignorato in modalita M5-only.")

    try:
        markets = parse_markets(args.markets)
        force_close_options = parse_force_close_options(args.force_close_options)
        breakout_windows = parse_breakout_windows(args.breakout_windows)
        orb_range_filters = parse_orb_range_filters(args.orb_range_filters)
        orb_range_quantiles = parse_orb_range_quantiles(args.orb_range_quantiles)
        rr_targets = parse_rr_targets(args.rr_targets)
        trade_direction_modes = parse_trade_direction_modes(args.trade_direction_modes)
        requested_audit_dates = parse_audit_dates(args.audit_dates)
    except ValueError as exc:
        print(f"Errore parametri: {exc}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    all_results = []

    audit_market_summaries: list[dict[str, Any]] = []
    audit_missing_orb_frames: list[pd.DataFrame] = []
    audit_orb_resample_frames: list[pd.DataFrame] = []
    audit_ambiguous_frames: list[pd.DataFrame] = []
    audit_trade_replay_frames: list[pd.DataFrame] = []

    for market_code in markets:
        market_label = MARKET_LABELS[market_code]
        market_scenarios = build_market_scenarios(
            market_code=market_code,
            force_close_options=force_close_options,
            breakout_windows=breakout_windows,
            orb_range_filters=orb_range_filters,
            rr_targets=rr_targets,
            trade_direction_modes=trade_direction_modes,
        )

        if not market_scenarios:
            print(f"Nessuno scenario per {market_label}")
            continue

        symbol = market_scenarios[0].symbol
        print("\n" + "#" * 100)
        print(f"BACKTEST MERCATO: {market_label} ({symbol})")
        print("#" * 100)

        market_csv_path = _resolve_market_csv_path(args, market_code)

        try:
            if market_csv_path:
                print(f"Uso sorgente CSV: {market_csv_path}")
                market_df_5m, data_diagnostics = load_m5_csv(
                    csv_path=market_csv_path,
                    csv_timezone=args.csv_timezone,
                )
            else:
                provider_raw = fetch_provider_data(symbol=symbol, period=args.period, interval=args.interval)
                market_df_5m, data_diagnostics = prepare_external_market_data(
                    raw_df=provider_raw,
                    source_timezone_fallback="UTC",
                )
        except Exception as exc:
            source_label = "CSV" if market_csv_path else "download"
            print(f"Errore {source_label} {market_label}: {exc}")
            continue

        missing_orb_df = audit_missing_orb_days(df_5m=market_df_5m, market_label=market_label)
        if missing_orb_df.empty:
            missing_orb_df = _empty_missing_orb_frame(market_label)

        missing_orb_path = output_dir / f"missing_orb_days_{market_slug(market_label)}.csv"
        missing_orb_df.to_csv(missing_orb_path, index=False)

        all_dates = sorted({ts.date() for ts in pd.DatetimeIndex(market_df_5m.index)})
        valid_orb_days = max(len(all_dates) - len(missing_orb_df), 0)

        _print_data_diagnostics(
            market_label=market_label,
            diagnostics=data_diagnostics,
            total_dates=len(all_dates),
            valid_orb_dates=valid_orb_days,
            missing_orb_dates=len(missing_orb_df),
        )
        print(f"  missing_orb_report: {missing_orb_path.name}")

        if args.audit_mode:
            selected_dates, out_of_range_dates = select_audit_dates(
                df_5m=market_df_5m,
                sample_days=args.audit_sample_days,
                requested_dates=requested_audit_dates,
            )
            orb_resample_audit_df = audit_orb_resampling(
                df_5m=market_df_5m,
                market_label=market_label,
                selected_dates=selected_dates,
            )

            audit_market_summary = build_dataset_audit_summary(
                market_label=market_label,
                df_5m=market_df_5m,
                diagnostics=data_diagnostics,
                missing_orb_days=missing_orb_df,
                selected_audit_dates=selected_dates,
                audit_dates_out_of_range=out_of_range_dates,
            )
            audit_market_summaries.append(audit_market_summary)
            audit_missing_orb_frames.append(missing_orb_df)
            audit_orb_resample_frames.append(orb_resample_audit_df)

            print(
                "Audit mode -> "
                f"missing_orb_days={len(missing_orb_df)}, "
                f"orb_resample_rows={len(orb_resample_audit_df)}, "
                f"sample_dates={len(selected_dates)}"
            )
            if out_of_range_dates:
                print(f"Audit mode -> date fuori range ignorate: {', '.join(out_of_range_dates)}")

        for scenario in market_scenarios:
            result = run_market_backtest(
                df=market_df_5m,
                scenario=scenario,
                max_trades_per_day=args.max_trades_per_day,
                initial_capital=args.capital,
                risk_per_trade=args.risk_per_trade,
                orb_range_class_config=orb_range_quantiles,
                audit_mode=args.audit_mode,
                audit_trades_limit=args.audit_trades_limit,
            )

            print("\n" + format_market_report(result))
            paths = save_scenario_outputs(result=result, output_dir=output_dir)
            print(summarize_outputs(paths))
            all_results.append(result)

            if args.audit_mode:
                if not result.audit_ambiguous_signals.empty:
                    audit_ambiguous_frames.append(result.audit_ambiguous_signals)
                if not result.audit_trade_replay.empty:
                    audit_trade_replay_frames.append(result.audit_trade_replay)

    if args.audit_mode:
        audit_summary_payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "audit_mode": True,
            "audit_options": {
                "audit_sample_days": args.audit_sample_days,
                "audit_dates": args.audit_dates,
                "audit_trades_limit": args.audit_trades_limit,
            },
            "markets": audit_market_summaries,
        }

        audit_missing_orb = (
            pd.concat(audit_missing_orb_frames, ignore_index=True)
            if audit_missing_orb_frames
            else pd.DataFrame()
        )
        audit_orb_resample = (
            pd.concat(audit_orb_resample_frames, ignore_index=True)
            if audit_orb_resample_frames
            else pd.DataFrame(columns=ORB_RESAMPLE_AUDIT_COLUMNS)
        )
        audit_ambiguous = (
            pd.concat(audit_ambiguous_frames, ignore_index=True)
            if audit_ambiguous_frames
            else pd.DataFrame(columns=AMBIGUOUS_AUDIT_COLUMNS)
        )
        audit_trade_replay = (
            pd.concat(audit_trade_replay_frames, ignore_index=True)
            if audit_trade_replay_frames
            else pd.DataFrame(columns=TRADE_REPLAY_AUDIT_COLUMNS)
        )

        audit_paths = save_audit_outputs(
            audit_dir=output_dir / "audit",
            dataset_summary=audit_summary_payload,
            missing_orb_days=audit_missing_orb,
            ambiguous_signal_candles=audit_ambiguous,
            orb_resample_audit=audit_orb_resample,
            trade_replay_audit=audit_trade_replay,
        )

        print("\nAudit files salvati:")
        for path in audit_paths.values():
            print(f"  - {path}")

    print("\n" + "=" * 100)
    print("PERFORMANCE PER MERCATO / SCENARIO")
    print("=" * 100)
    table = performance_per_market(all_results)
    if table.empty:
        print("Nessun risultato disponibile.")
        return

    comparison_path = output_dir / "comparison_summary.csv"
    table.to_csv(comparison_path, index=False)
    print(f"Comparison summary salvato: {comparison_path}")

    primary, secondary = split_primary_secondary(table)

    if not primary.empty:
        print(
            "\nScenari focus (SP500/NASDAQ, 10:00|10:30, all|small|small+large, RR 1.0|1.5, both):"
        )
        print(primary.to_string(index=False))

    if not secondary.empty:
        print("\nAltri scenari eseguiti:")
        print(secondary.to_string(index=False))


if __name__ == "__main__":
    main()
