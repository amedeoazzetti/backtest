"""
CLI principale per backtest ORB v1.6.

Esempi:
    python main.py
    python main.py --period 60d --interval 5m --markets SP500
    python main.py --force-close-options none --breakout-windows 10:30 --orb-range-filters small,small+large --rr-targets 1.0,1.5 --trade-direction-modes both,long_only,short_only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("Installa yfinance: pip install yfinance")
    sys.exit(1)

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
    parse_breakout_windows,
    parse_force_close_options,
    parse_markets,
    parse_orb_range_filters,
    parse_orb_range_quantiles,
    parse_rr_targets,
    parse_trade_direction_modes,
)
from reporting import save_scenario_outputs, split_primary_secondary, summarize_outputs


def fetch_data(symbol: str, period: str, interval: str) -> pd.DataFrame:
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

    df.columns = [str(c).lower() for c in df.columns]
    required = ["open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonne mancanti per {symbol}: {', '.join(missing)}")

    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest ORB v1.6")
    parser.add_argument("--period", default="120d", help="Periodo storico (default: 120d)")
    parser.add_argument("--interval", default="5m", help="Timeframe dati (default: 5m)")
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

    try:
        markets = parse_markets(args.markets)
        force_close_options = parse_force_close_options(args.force_close_options)
        breakout_windows = parse_breakout_windows(args.breakout_windows)
        orb_range_filters = parse_orb_range_filters(args.orb_range_filters)
        orb_range_quantiles = parse_orb_range_quantiles(args.orb_range_quantiles)
        rr_targets = parse_rr_targets(args.rr_targets)
        trade_direction_modes = parse_trade_direction_modes(args.trade_direction_modes)
    except ValueError as exc:
        print(f"Errore parametri: {exc}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    all_results = []

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

        try:
            market_df = fetch_data(symbol=symbol, period=args.period, interval=args.interval)
        except Exception as exc:
            print(f"Errore download {market_label}: {exc}")
            continue

        for scenario in market_scenarios:
            result = run_market_backtest(
                df=market_df,
                scenario=scenario,
                max_trades_per_day=args.max_trades_per_day,
                initial_capital=args.capital,
                risk_per_trade=args.risk_per_trade,
                orb_range_class_config=orb_range_quantiles,
            )

            print("\n" + format_market_report(result))
            paths = save_scenario_outputs(result=result, output_dir=output_dir)
            print(summarize_outputs(paths))
            all_results.append(result)

    print("\n" + "=" * 100)
    print("PERFORMANCE PER MERCATO / SCENARIO")
    print("=" * 100)
    table = performance_per_market(all_results)
    if table.empty:
        print("Nessun risultato disponibile.")
        return

    primary, secondary = split_primary_secondary(table)

    if not primary.empty:
        print(
            "\nScenari focus v1.6 (SP500, no_time_close, 10:30, "
            "small|small+large, RR 1.0|1.5, both|long_only|short_only):"
        )
        print(primary.to_string(index=False))

    if not secondary.empty:
        print("\nAltri scenari eseguiti:")
        print(secondary.to_string(index=False))


if __name__ == "__main__":
    main()
