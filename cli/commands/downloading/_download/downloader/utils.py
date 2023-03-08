from dataclasses import dataclass
from datetime import datetime

from pandas_market_calendars import get_calendar
from pandas_market_calendars.calendar_registry import MarketCalendar


@dataclass
class LoadMarketCalendarConfig:
    start_date: str
    end_date: str
    exchange: str = "NYSE"
    column_name: str = "market_open"


def load_market_calendar(cfg: LoadMarketCalendarConfig) -> MarketCalendar:
    return get_calendar(cfg.exchange).schedule(
        start_date=str(cfg.start_date), end_date=str(cfg.end_date)
    )[cfg.column_name]
