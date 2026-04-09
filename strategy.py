"""
Opening Range Breakout (ORB) strategy engine.

Regole implementate:
- Timezone ufficiale: America/New_York
- ORB M15: 09:30-09:45
- Segnali M5 validi: 09:45-11:00
- Entry su open candela successiva alla breakout candle
- Stop sul lato opposto del range e TP fisso 1:1
- Max 2 trade al giorno
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import time
import math
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd


TRADE_COLUMNS = [
    "date",
    "market",
    "direction",
    "breakout_window_label",
    "orb_high",
    "orb_low",
    "orb_range",
    "orb_range_points",
    "orb_range_pct_of_entry",
    "orb_range_class",
    "breakout_candle_time",
    "breakout_time_ny",
    "minutes_from_0945_to_breakout",
    "entry_time",
    "entry_time_ny",
    "entry_price",
    "stop_loss",
    "take_profit",
    "exit_time",
    "exit_time_ny",
    "exit_price",
    "exit_reason",
    "risk_points",
    "reward_points",
    "result_points",
    "result_r",
    "trade_duration_minutes",
    "first_breakout_side",
    "did_price_touch_both_sides_before_entry",
]


@dataclass(frozen=True)
class ORBConfig:
    market: str
    timezone: str = "America/New_York"
    orb_start: time = time(9, 30)
    orb_end: time = time(9, 45)
    signal_start: time = time(9, 45)
    signal_end: time = time(11, 0)
    breakout_window_label: Optional[str] = None
    max_trades_per_day: int = 2
    force_close_time: Optional[time] = None


@dataclass
class TradeRecord:
    date: str
    market: str
    direction: str
    breakout_window_label: str
    orb_high: float
    orb_low: float
    orb_range: float
    orb_range_points: float
    orb_range_pct_of_entry: float
    orb_range_class: str
    breakout_candle_time: pd.Timestamp
    breakout_time_ny: str
    minutes_from_0945_to_breakout: float
    entry_time: pd.Timestamp
    entry_time_ny: str
    entry_price: float
    stop_loss: float
    take_profit: float
    exit_time: pd.Timestamp
    exit_time_ny: str
    exit_price: float
    exit_reason: str
    risk_points: float
    reward_points: float
    result_points: float
    result_r: float
    trade_duration_minutes: float
    first_breakout_side: Optional[str]
    did_price_touch_both_sides_before_entry: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OpeningRangeBreakoutStrategy:
    def __init__(self, config: ORBConfig):
        self.config = config
        self.ny_tz = ZoneInfo(config.timezone)
        self.breakout_window_label = config.breakout_window_label or (
            f"breakout_window_{config.signal_start.strftime('%H%M')}_{config.signal_end.strftime('%H%M')}"
        )
        self.current_day = None
        self.open_trade: Optional[dict[str, Any]] = None
        self.reset_day()

        self.ambiguous_signal_candles = 0
        self.invalid_risk_entries = 0
        self.days_missing_orb = 0
        self.rows_dropped_nan = 0

    def reset_day(self) -> None:
        self.orb_high: Optional[float] = None
        self.orb_low: Optional[float] = None
        self.orb_range: Optional[float] = None

        self.trade_count = 0
        self.direction_taken: Optional[str] = None
        self.session_active = True

        self.first_breakout_side: Optional[str] = None
        self.day_touch_upper = False
        self.day_touch_lower = False

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("Il DataFrame deve avere un DatetimeIndex.")

        working = df.copy()
        working.columns = [str(c).lower() for c in working.columns]
        required = {"open", "high", "low", "close"}
        missing = required.difference(working.columns)
        if missing:
            missing_cols = ", ".join(sorted(missing))
            raise ValueError(f"Colonne OHLC mancanti: {missing_cols}")

        working = working.sort_index()
        working = working[~working.index.duplicated(keep="last")]

        if working.index.tz is None:
            working.index = working.index.tz_localize("UTC")

        working.index = working.index.tz_convert(self.ny_tz)

        rows_before = len(working)
        working = working.dropna(subset=["open", "high", "low", "close"])
        self.rows_dropped_nan = rows_before - len(working)

        # Dato sporco: high < low invalida completamente la candela.
        working = working[working["high"] >= working["low"]]
        return working

    def set_opening_range(self, day_frame: pd.DataFrame) -> bool:
        # ORB M15 ricavata dai dati M5 tramite resample a 15 minuti.
        m15 = (
            day_frame.resample("15min", label="left", closed="left")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last"})
            .dropna(subset=["open", "high", "low", "close"])
        )

        orb_rows = m15[m15.index.time == self.config.orb_start]
        if orb_rows.empty:
            self.session_active = False
            return False

        orb_candle = orb_rows.iloc[0]
        self.orb_high = float(orb_candle["high"])
        self.orb_low = float(orb_candle["low"])
        self.orb_range = self.orb_high - self.orb_low

        if self.orb_range <= 0:
            self.session_active = False
            return False

        return True

    def check_long_signal(self, m5_candle: pd.Series) -> bool:
        return bool(
            self.orb_high is not None
            and m5_candle["high"] > self.orb_high
            and m5_candle["close"] > self.orb_high
        )

    def check_short_signal(self, m5_candle: pd.Series) -> bool:
        return bool(
            self.orb_low is not None
            and m5_candle["low"] < self.orb_low
            and m5_candle["close"] < self.orb_low
        )

    def _update_breakout_state(self, m5_candle: pd.Series) -> tuple[bool, bool]:
        touched_upper = bool(self.orb_high is not None and m5_candle["high"] > self.orb_high)
        touched_lower = bool(self.orb_low is not None and m5_candle["low"] < self.orb_low)

        if self.first_breakout_side is None:
            if touched_upper and touched_lower:
                self.first_breakout_side = "both_same_candle"
            elif touched_upper:
                self.first_breakout_side = "long"
            elif touched_lower:
                self.first_breakout_side = "short"

        self.day_touch_upper = self.day_touch_upper or touched_upper
        self.day_touch_lower = self.day_touch_lower or touched_lower
        return touched_upper, touched_lower

    def create_long_trade(
        self,
        next_candle_time: pd.Timestamp,
        next_candle_open: float,
        breakout_candle_time: pd.Timestamp,
    ) -> bool:
        if self.orb_low is None:
            return False

        entry_price = float(next_candle_open)
        stop_loss = float(self.orb_low)
        risk = entry_price - stop_loss
        if risk <= 0:
            self.invalid_risk_entries += 1
            return False

        take_profit = entry_price + risk
        orb_range_points = float(self.orb_range)
        orb_range_pct_of_entry = (
            (orb_range_points / entry_price) * 100.0
            if entry_price and not math.isclose(entry_price, 0.0)
            else float("nan")
        )

        self.open_trade = {
            "direction": "LONG",
            "entry_time": next_candle_time,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": float(take_profit),
            "breakout_candle_time": breakout_candle_time,
            "risk_points": float(risk),
            "reward_points": float(risk),
            "orb_high": float(self.orb_high),
            "orb_low": float(self.orb_low),
            "orb_range": float(self.orb_range),
            "orb_range_points": orb_range_points,
            "orb_range_pct_of_entry": float(orb_range_pct_of_entry),
            "first_breakout_side": self.first_breakout_side,
            "did_price_touch_both_sides_before_entry": bool(
                self.day_touch_upper and self.day_touch_lower
            ),
            "breakout_window_label": self.breakout_window_label,
        }
        self.trade_count += 1
        self.direction_taken = "LONG"
        if self.trade_count >= self.config.max_trades_per_day:
            self.session_active = False
        return True

    def create_short_trade(
        self,
        next_candle_time: pd.Timestamp,
        next_candle_open: float,
        breakout_candle_time: pd.Timestamp,
    ) -> bool:
        if self.orb_high is None:
            return False

        entry_price = float(next_candle_open)
        stop_loss = float(self.orb_high)
        risk = stop_loss - entry_price
        if risk <= 0:
            self.invalid_risk_entries += 1
            return False

        take_profit = entry_price - risk
        orb_range_points = float(self.orb_range)
        orb_range_pct_of_entry = (
            (orb_range_points / entry_price) * 100.0
            if entry_price and not math.isclose(entry_price, 0.0)
            else float("nan")
        )

        self.open_trade = {
            "direction": "SHORT",
            "entry_time": next_candle_time,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "take_profit": float(take_profit),
            "breakout_candle_time": breakout_candle_time,
            "risk_points": float(risk),
            "reward_points": float(risk),
            "orb_high": float(self.orb_high),
            "orb_low": float(self.orb_low),
            "orb_range": float(self.orb_range),
            "orb_range_points": orb_range_points,
            "orb_range_pct_of_entry": float(orb_range_pct_of_entry),
            "first_breakout_side": self.first_breakout_side,
            "did_price_touch_both_sides_before_entry": bool(
                self.day_touch_upper and self.day_touch_lower
            ),
            "breakout_window_label": self.breakout_window_label,
        }
        self.trade_count += 1
        self.direction_taken = "SHORT"
        if self.trade_count >= self.config.max_trades_per_day:
            self.session_active = False
        return True

    def close_trade(self, reason: str, price: float, close_time: pd.Timestamp) -> TradeRecord:
        if self.open_trade is None:
            raise RuntimeError("close_trade chiamato senza trade aperto.")

        trade = self.open_trade
        entry_price = float(trade["entry_price"])
        exit_price = float(price)
        direction = trade["direction"]

        if direction == "LONG":
            result_points = exit_price - entry_price
        else:
            result_points = entry_price - exit_price

        risk_points = float(trade["risk_points"])
        result_r = result_points / risk_points if risk_points > 0 else 0.0
        duration_minutes = float((close_time - trade["entry_time"]).total_seconds() / 60.0)

        reference_0945 = pd.Timestamp.combine(
            trade["breakout_candle_time"].date(),
            self.config.signal_start,
        ).tz_localize(self.ny_tz)
        minutes_from_0945 = float(
            (trade["breakout_candle_time"] - reference_0945).total_seconds() / 60.0
        )

        record = TradeRecord(
            date=str(trade["entry_time"].date()),
            market=self.config.market,
            direction=direction,
            breakout_window_label=str(trade["breakout_window_label"]),
            orb_high=float(trade["orb_high"]),
            orb_low=float(trade["orb_low"]),
            orb_range=float(trade["orb_range"]),
            orb_range_points=float(trade["orb_range_points"]),
            orb_range_pct_of_entry=float(trade["orb_range_pct_of_entry"]),
            orb_range_class="unclassified",
            breakout_candle_time=trade["breakout_candle_time"],
            breakout_time_ny=trade["breakout_candle_time"].strftime("%Y-%m-%d %H:%M:%S %Z"),
            minutes_from_0945_to_breakout=minutes_from_0945,
            entry_time=trade["entry_time"],
            entry_time_ny=trade["entry_time"].strftime("%Y-%m-%d %H:%M:%S %Z"),
            entry_price=entry_price,
            stop_loss=float(trade["stop_loss"]),
            take_profit=float(trade["take_profit"]),
            exit_time=close_time,
            exit_time_ny=close_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
            exit_price=exit_price,
            exit_reason=reason,
            risk_points=risk_points,
            reward_points=float(trade["reward_points"]),
            result_points=float(result_points),
            result_r=float(result_r),
            trade_duration_minutes=duration_minutes,
            first_breakout_side=trade["first_breakout_side"],
            did_price_touch_both_sides_before_entry=bool(
                trade["did_price_touch_both_sides_before_entry"]
            ),
        )

        self.open_trade = None
        return record

    def manage_open_trade(
        self,
        candle_time: pd.Timestamp,
        candle: pd.Series,
    ) -> Optional[TradeRecord]:
        if self.open_trade is None:
            return None

        force_close_time = self.config.force_close_time
        candle_open = float(candle["open"])
        candle_high = float(candle["high"])
        candle_low = float(candle["low"])

        if force_close_time is not None and candle_time.time() >= force_close_time:
            return self.close_trade("time_close", candle_open, candle_time)

        direction = self.open_trade["direction"]
        stop_loss = float(self.open_trade["stop_loss"])
        take_profit = float(self.open_trade["take_profit"])

        if direction == "LONG":
            if candle_open <= stop_loss:
                return self.close_trade("sl", candle_open, candle_time)
            if candle_open >= take_profit:
                return self.close_trade("tp", candle_open, candle_time)

            hit_sl = candle_low <= stop_loss
            hit_tp = candle_high >= take_profit

            # Ambiguita' intrabar: scelta conservativa (SL).
            if hit_sl and hit_tp:
                return self.close_trade("sl", stop_loss, candle_time)
            if hit_sl:
                return self.close_trade("sl", stop_loss, candle_time)
            if hit_tp:
                return self.close_trade("tp", take_profit, candle_time)

        else:  # SHORT
            if candle_open >= stop_loss:
                return self.close_trade("sl", candle_open, candle_time)
            if candle_open <= take_profit:
                return self.close_trade("tp", candle_open, candle_time)

            hit_sl = candle_high >= stop_loss
            hit_tp = candle_low <= take_profit

            # Ambiguita' intrabar: scelta conservativa (SL).
            if hit_sl and hit_tp:
                return self.close_trade("sl", stop_loss, candle_time)
            if hit_sl:
                return self.close_trade("sl", stop_loss, candle_time)
            if hit_tp:
                return self.close_trade("tp", take_profit, candle_time)

        return None

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        data = self.prepare_dataframe(df)
        if data.empty:
            empty = pd.DataFrame(columns=TRADE_COLUMNS)
            diagnostics = {
                "ambiguous_signal_candles": 0,
                "invalid_risk_entries": 0,
                "days_processed": 0,
            }
            return empty, diagnostics

        day_groups = {day: frame for day, frame in data.groupby(data.index.date)}
        trades: list[TradeRecord] = []

        for i in range(len(data)):
            candle_time = data.index[i]
            candle = data.iloc[i]
            session_day = candle_time.date()

            if self.current_day != session_day:
                self.reset_day()
                self.current_day = session_day
                day_frame = day_groups.get(session_day)
                if day_frame is not None:
                    has_orb = self.set_opening_range(day_frame)
                    if not has_orb:
                        self.days_missing_orb += 1

            closed_trade = self.manage_open_trade(candle_time, candle)
            if closed_trade is not None:
                trades.append(closed_trade)
                continue

            if self.open_trade is not None:
                continue

            if self.orb_high is None or self.orb_low is None:
                continue

            clock = candle_time.time()
            if clock < self.config.signal_start:
                continue

            if clock >= self.config.signal_end:
                self.session_active = False
                continue

            if not self.session_active:
                continue

            if self.trade_count >= self.config.max_trades_per_day:
                self.session_active = False
                continue

            if i + 1 >= len(data):
                continue

            next_candle_time = data.index[i + 1]
            next_candle = data.iloc[i + 1]
            if next_candle_time.date() != session_day:
                continue

            touched_upper, touched_lower = self._update_breakout_state(candle)
            if touched_upper and touched_lower:
                self.ambiguous_signal_candles += 1
                continue

            if self.check_long_signal(candle):
                self.create_long_trade(
                    next_candle_time=next_candle_time,
                    next_candle_open=float(next_candle["open"]),
                    breakout_candle_time=candle_time,
                )
                continue

            if self.check_short_signal(candle):
                self.create_short_trade(
                    next_candle_time=next_candle_time,
                    next_candle_open=float(next_candle["open"]),
                    breakout_candle_time=candle_time,
                )

        if self.open_trade is not None:
            last_time = data.index[-1]
            last_price = float(data.iloc[-1]["close"])
            trades.append(self.close_trade("time_close", last_price, last_time))

        if trades:
            trades_df = pd.DataFrame([t.to_dict() for t in trades])
            trades_df = trades_df[TRADE_COLUMNS].sort_values("entry_time").reset_index(drop=True)
        else:
            trades_df = pd.DataFrame(columns=TRADE_COLUMNS)

        diagnostics = {
            "ambiguous_signal_candles": int(self.ambiguous_signal_candles),
            "invalid_risk_entries": int(self.invalid_risk_entries),
            "days_missing_orb": int(self.days_missing_orb),
            "rows_dropped_nan": int(self.rows_dropped_nan),
            "days_processed": len(day_groups),
        }
        return trades_df, diagnostics
