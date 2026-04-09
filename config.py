from __future__ import annotations

from dataclasses import dataclass
from datetime import time
import math
from typing import Optional


MARKET_SYMBOLS = {
    "NASDAQ": "^IXIC",
    "SP500": "^GSPC",
}

MARKET_LABELS = {
    "NASDAQ": "NASDAQ",
    "SP500": "S&P 500",
}

DEFAULT_MARKETS = "SP500"
DEFAULT_FORCE_CLOSE_OPTIONS = "none"
DEFAULT_BREAKOUT_WINDOWS = "10:30"
DEFAULT_ORB_RANGE_FILTERS = "small,small+large"
DEFAULT_RR_TARGETS = "1.0,1.5"
DEFAULT_TRADE_DIRECTION_MODES = "both,long_only,short_only"

PRIMARY_FOCUS_BREAKOUT_WINDOWS = {
    "breakout_window_0945_1030",
}
PRIMARY_FOCUS_ORB_FILTERS = {"small", "small+large"}
PRIMARY_FOCUS_RR_TARGETS = {1.0, 1.5}
PRIMARY_FOCUS_TRADE_DIRECTION_MODES = {"both", "long_only", "short_only"}

VALID_ORB_RANGE_CLASSES = {"small", "medium", "large"}
VALID_TRADE_DIRECTION_MODES = {"both", "long_only", "short_only"}


@dataclass(frozen=True)
class ORBRangeFilterConfig:
    label: str
    allowed_classes: Optional[frozenset[str]]

    @property
    def slug(self) -> str:
        if self.label == "all":
            return "all"
        return self.label.replace("+", "_plus_")


@dataclass(frozen=True)
class ORBRangeClassResult:
    lower: Optional[float]
    upper: Optional[float]
    method: str
    quantiles: Optional[tuple[float, float]] = None


@dataclass(frozen=True)
class ScenarioConfig:
    market_code: str
    market_label: str
    symbol: str
    force_close_time: Optional[time]
    breakout_window_end: time
    orb_range_filter: ORBRangeFilterConfig
    rr_target: float
    trade_direction_mode: str

    @property
    def force_close_label(self) -> str:
        return force_close_label(self.force_close_time)

    @property
    def breakout_window_label(self) -> str:
        return breakout_window_label(self.breakout_window_end)

    @property
    def orb_range_filter_label(self) -> str:
        return self.orb_range_filter.label

    @property
    def orb_range_filter_slug(self) -> str:
        return self.orb_range_filter.slug

    @property
    def allowed_orb_range_classes(self) -> Optional[frozenset[str]]:
        return self.orb_range_filter.allowed_classes

    @property
    def rr_target_label(self) -> str:
        return rr_target_label(self.rr_target)

    @property
    def trade_direction_mode_label(self) -> str:
        return self.trade_direction_mode

    @property
    def scenario_label(self) -> str:
        return (
            f"{self.breakout_window_label}_{self.force_close_label}_"
            f"orb_{self.orb_range_filter_slug}_{self.rr_target_label}_{self.trade_direction_mode_label}"
        )


@dataclass(frozen=True)
class ORBRangeClassConfig:
    lower_quantile: float = 0.33
    upper_quantile: float = 0.66


def rr_target_slug(rr_target: float) -> str:
    formatted = f"{rr_target:.4f}".rstrip("0").rstrip(".")
    if "." not in formatted:
        formatted = f"{formatted}.0"
    return formatted.replace(".", "_")


def rr_target_label(rr_target: float) -> str:
    return f"rr_{rr_target_slug(rr_target)}"


def parse_rr_targets(raw_targets: str) -> list[float]:
    targets: list[float] = []
    for token in raw_targets.split(","):
        value = token.strip()
        if not value:
            continue

        try:
            parsed = float(value)
        except Exception as exc:
            raise ValueError(f"RR target non valido: {token}") from exc

        if not math.isfinite(parsed) or parsed <= 0:
            raise ValueError(f"RR target non valido: {token}. Deve essere > 0")

        targets.append(parsed)

    if not targets:
        raise ValueError("Nessun rr target valido specificato.")

    unique: list[float] = []
    seen = set()
    for item in targets:
        if item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


def parse_trade_direction_modes(raw_modes: str) -> list[str]:
    modes: list[str] = []
    for token in raw_modes.split(","):
        mode = token.strip().lower()
        if not mode:
            continue

        if mode not in VALID_TRADE_DIRECTION_MODES:
            raise ValueError(
                f"Trade direction mode non valido: {token}. "
                "Valori supportati: both, long_only, short_only"
            )

        modes.append(mode)

    if not modes:
        raise ValueError("Nessun trade direction mode valido specificato.")

    unique = []
    seen = set()
    for mode in modes:
        if mode not in seen:
            unique.append(mode)
            seen.add(mode)
    return unique


def _normalize_orb_filter_item(raw_item: str) -> str:
    item = raw_item.strip().lower().replace(" ", "")
    if not item:
        raise ValueError("Valore vuoto in --orb-range-filters")

    if item == "all":
        return "all"

    parts = item.split("+")
    if not parts or any(not p for p in parts):
        raise ValueError(f"Filtro ORB non valido: {raw_item}")

    unique_parts = []
    seen = set()
    for part in parts:
        if part not in VALID_ORB_RANGE_CLASSES:
            raise ValueError(
                f"Classe ORB non valida: {part}. Valori supportati: small, medium, large"
            )
        if part not in seen:
            unique_parts.append(part)
            seen.add(part)

    ordered = [c for c in ["small", "medium", "large"] if c in unique_parts]
    return "+".join(ordered)


def parse_orb_range_filters(raw_filters: str) -> list[ORBRangeFilterConfig]:
    filters: list[ORBRangeFilterConfig] = []
    for token in raw_filters.split(","):
        cleaned = _normalize_orb_filter_item(token)

        if cleaned == "all":
            cfg = ORBRangeFilterConfig(label="all", allowed_classes=None)
        else:
            cfg = ORBRangeFilterConfig(
                label=cleaned,
                allowed_classes=frozenset(cleaned.split("+")),
            )
        filters.append(cfg)

    if not filters:
        raise ValueError("Nessun filtro ORB range valido specificato.")

    unique = []
    seen = set()
    for item in filters:
        key = item.label
        if key not in seen:
            unique.append(item)
            seen.add(key)
    return unique


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
    orb_range_filters: list[ORBRangeFilterConfig],
    rr_targets: list[float],
    trade_direction_modes: list[str],
) -> list[ScenarioConfig]:
    market_label = MARKET_LABELS[market_code]
    symbol = MARKET_SYMBOLS[market_code]

    scenarios = []
    for breakout_window_end in breakout_windows:
        for force_close_time in force_close_options:
            for orb_range_filter in orb_range_filters:
                for rr_target in rr_targets:
                    for trade_direction_mode in trade_direction_modes:
                        scenarios.append(
                            ScenarioConfig(
                                market_code=market_code,
                                market_label=market_label,
                                symbol=symbol,
                                force_close_time=force_close_time,
                                breakout_window_end=breakout_window_end,
                                orb_range_filter=orb_range_filter,
                                rr_target=rr_target,
                                trade_direction_mode=trade_direction_mode,
                            )
                        )

    return scenarios
