from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from typing import Optional


MARKET_SYMBOLS = {
    "NASDAQ": "^IXIC",
    "SP500": "^GSPC",
}

MARKET_LABELS = {
    "NASDAQ": "NASDAQ",
    "SP500": "S&P 500",
}

DEFAULT_MARKETS = "NASDAQ"
DEFAULT_FORCE_CLOSE_OPTIONS = "none,12:00"
DEFAULT_BREAKOUT_WINDOWS = "11:00,10:30,10:00"


@dataclass(frozen=True)
class ScenarioConfig:
    market_code: str
    market_label: str
    symbol: str
    force_close_time: Optional[time]
    breakout_window_end: time

    @property
    def force_close_label(self) -> str:
        return force_close_label(self.force_close_time)

    @property
    def breakout_window_label(self) -> str:
        return breakout_window_label(self.breakout_window_end)

    @property
    def scenario_label(self) -> str:
        return f"{self.breakout_window_label}_{self.force_close_label}"


@dataclass(frozen=True)
class ORBRangeClassConfig:
    lower_quantile: float = 0.33
    upper_quantile: float = 0.66


def normalize_market_name(raw_name: str) -> str:
    normalized = raw_name.strip().upper().replace(" ", "")
    if normalized in {"NASDAQ", "IXIC"}:
        return "NASDAQ"
    if normalized in {"SP500", "S&P500", "S&P_500", "GSPC", "SPX"}:
        return "SP500"
    raise ValueError(f"Mercato non supportato: {raw_name}")


def parse_markets(raw_markets: str) -> list[str]:
    markets = []
    for token in raw_markets.split(","):
        value = token.strip()
        if value:
            markets.append(normalize_market_name(value))

    if not markets:
        raise ValueError("Nessun mercato valido specificato.")

    return list(dict.fromkeys(markets))


def parse_force_close_options(raw_options: str) -> list[Optional[time]]:
    results = []
    for token in raw_options.split(","):
        value = token.strip().lower()
        if not value:
            continue

        if value in {"none", "no", "off"}:
            results.append(None)
            continue

        try:
            hour_str, minute_str = value.split(":")
            parsed = time(int(hour_str), int(minute_str))
        except Exception as exc:
            raise ValueError(f"Orario force-close non valido: {token}") from exc

        results.append(parsed)

    if not results:
        raise ValueError("Nessuna opzione force-close valida specificata.")

    return results


def parse_breakout_windows(
    raw_windows: str,
    signal_start: time = time(9, 45),
) -> list[time]:
    windows = []
    for token in raw_windows.split(","):
        value = token.strip()
        if not value:
            continue

        try:
            hour_str, minute_str = value.split(":")
            parsed = time(int(hour_str), int(minute_str))
        except Exception as exc:
            raise ValueError(f"Breakout window non valida: {token}") from exc

        if parsed <= signal_start:
            raise ValueError(
                f"Breakout window {value} non valida: deve essere > {signal_start.strftime('%H:%M')}"
            )
        windows.append(parsed)

    if not windows:
        raise ValueError("Nessuna breakout window valida specificata.")

    unique = []
    seen = set()
    for item in windows:
        if item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


def parse_orb_range_quantiles(raw_quantiles: str) -> ORBRangeClassConfig:
    try:
        lower_str, upper_str = [x.strip() for x in raw_quantiles.split(",")]
        lower = float(lower_str)
        upper = float(upper_str)
    except Exception as exc:
        raise ValueError(
            "Formato non valido per --orb-range-quantiles. Usa ad esempio: 0.33,0.66"
        ) from exc

    if not (0.0 < lower < upper < 1.0):
        raise ValueError("I quantili ORB range devono rispettare: 0 < lower < upper < 1")

    return ORBRangeClassConfig(lower_quantile=lower, upper_quantile=upper)


def breakout_window_label(window_end: time, signal_start: time = time(9, 45)) -> str:
    return f"breakout_window_{signal_start.strftime('%H%M')}_{window_end.strftime('%H%M')}"


def force_close_label(force_close: Optional[time]) -> str:
    if force_close is None:
        return "no_time_close"
    return f"time_close_{force_close.strftime('%H%M')}"


def market_slug(market_label: str) -> str:
    cleaned = market_label.lower().replace(" ", "")
    if "nasdaq" in cleaned:
        return "nasdaq"
    if "s&p" in cleaned or "sp500" in cleaned or "sandp500" in cleaned:
        return "sp500"
    return cleaned.replace("&", "and").replace(".", "")


def build_market_scenarios(
    market_code: str,
    force_close_options: list[Optional[time]],
    breakout_windows: list[time],
) -> list[ScenarioConfig]:
    market_label = MARKET_LABELS[market_code]
    symbol = MARKET_SYMBOLS[market_code]

    scenarios = []
    for breakout_window_end in breakout_windows:
        for force_close_time in force_close_options:
            scenarios.append(
                ScenarioConfig(
                    market_code=market_code,
                    market_label=market_label,
                    symbol=symbol,
                    force_close_time=force_close_time,
                    breakout_window_end=breakout_window_end,
                )
            )

    return scenarios
