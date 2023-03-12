from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple

import pandas as pd
from pandas_market_calendars import get_calendar
from pandas_market_calendars.calendar_registry import MarketCalendar

_logger = logging.getLogger(__name__)


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


def to_datetime_mapping(
    raw_data: List[Tuple[datetime, object]]
) -> Dict[datetime, object]:
    mapping = {}
    for dt, obj in raw_data:
        key = datetime_key(dt)

        if key in mapping:
            import pdb

            pdb.set_trace()
            _logger.warn(
                f"found a duplicate datetime key while building mapping: ({str(key)}, {obj})"
            )
        else:
            mapping[key] = obj
    return mapping


def datetime_key(dt: datetime) -> datetime:
    return dt.replace(microsecond=0, second=0)


def load_quarterly_calender(years_examined: int) -> List[datetime]:
    target_dates = ["Jan 1", "Apr 1", "Jul 1", "Oct 1"]

    now = datetime.now()
    current_year = now.year
    cutoff_time = now - timedelta(hours=24 * 365 * years_examined)

    calendar_dts = []
    for i in range(years_examined + 1):
        for date in target_dates:
            loop_year = current_year - i
            loop_dt = datetime.strptime(f"{date} {loop_year}", "%b %d %Y")

            if loop_dt >= cutoff_time and loop_dt <= now:
                calendar_dts.append(loop_dt)

    return calendar_dts
