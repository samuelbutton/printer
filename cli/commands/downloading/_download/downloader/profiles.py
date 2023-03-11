from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from os.path import exists
from typing import Dict, List, Optional, Set, Tuple

from polygon import RESTClient
import pandas as pd
from pandas_market_calendars.calendar_registry import MarketCalendar
import pytz

from ..configs.download import DownloadConfig
from .downloader import Downloader
from .db import DatabaseInterface, DataframeDatabase
from .utils import (
    datetime_key,
    load_quarterly_calender,
    to_datetime_mapping,
)

_logger = logging.getLogger(__name__)


class ProfilesDownloader(Downloader):
    def __init__(self, cfg: DownloadConfig):
        self.cfg: DownloadConfig = cfg
        if cfg.symbols_limit is not None:
            self.cfg.symbols = self.cfg.symbols[: cfg.symbols_limit]

        self._polygon_client: Optional[RESTClient] = None

        self._database: Optional[DatabaseInterface] = None
        if cfg.use_existing_db and exists(cfg.database_filepath):
            _logger.info("loading existing database")
            self._database = DataframeDatabase()
            self._database.load(cfg.database_filepath)

        self.target_datetimes = load_quarterly_calender(cfg.years_examined)

    @property
    def polygon_client(self) -> RESTClient:
        if self._polygon_client is None:
            _logger.info("loading polygon client")
            self._polygon_client = RESTClient(api_key=self.cfg.polygon_api_key)
        return self._polygon_client

    @property
    def database(self) -> DatabaseInterface:
        if self._database is None:
            _logger.info("loading new database")
            self._database = DataframeDatabase()
        return self._database

    def find_missing_dates(self, symbol: str) -> List[datetime.date]:
        _logger.debug(f"Finding missing dates for {symbol}")
        cfg = GetDatabaseMissesConfig(
            symbol=symbol,
            database=self.database,
            datetimes=self.target_datetimes,
        )
        cfg.set_defaults()
        return get_profile_misses(cfg)

    def pull_missing_data(
        self, symbol: str, missing_datetimes: List[datetime]
    ) -> List[Tuple[datetime, object]]:
        _logger.debug(f"Pulling missing data: {symbol}")
        return get_profiles(
            GetProfilesConfig(
                symbol,
                self.polygon_client,
                missing_datetimes,
            )
        )

    def save_to_database(
        self,
        symbol: str,
        missing_datetimes: List[datetime],
        pulled_data: List[Tuple[datetime, object]],
    ) -> None:
        if pulled_data is None or len(missing_datetimes) == 0 or len(pulled_data) == 0:
            return

        dt_to_profile = to_datetime_mapping(pulled_data)

        cfg = UpdateDatabaseConfig(
            symbol,
            self.database,
            missing_datetimes,
            dt_to_profile,
            self.cfg.database_entry_type,
        )
        update_count = fill_new_entries(cfg)
        new_rows = generate_new_rows(cfg)

        has_new_rows = len(new_rows) > 0
        if has_new_rows > 0:
            self.database.add_rows(symbol, new_rows)

        if update_count > 0 or has_new_rows:
            _logger.debug(f"Saving to database: {symbol}")
            self.database.save(self.cfg.database_filepath)

    def symbols(self) -> List[str]:
        return self.cfg.symbols


def get_ignored_sp500_equity_dates() -> Set[str]:
    return set(
        [
            "2019-11-29",
            "2019-12-24",
            "2020-11-27",
            "2020-12-24",
            "2017-11-24",
            "2018-07-03",
            "2018-11-23",
            "2018-12-24",
            "2019-07-03",
            "2021-11-26",
            "2022-11-25",
            "2022-02-24",
        ]
    )


def get_ignored_sp500_symbols():
    return set(
        [
            "ANTM",
            "BLL",
            "CERN",
            "CTXS",
            "DISCA",
            "DISCK",
            "FB",
            "INFO",
            "KSU",
            "PBCT",
            "VIAC",
            "WLTW",
            "XLNX",
            "DRE",
            "AOS",
            "OGN",
            "PENN",
            "PM",
            "PRU",
            "PHM",
            "CARR",
            "HWM",
            "OTIS",
            "TWTR",
            "VTRS",
            "ABMD",
        ]
    )


@dataclass
class GetDatabaseMissesConfig:
    symbol: str
    database: DatabaseInterface
    datetimes: List[datetime]
    ignored_date_strs: Set[str] = None
    ignored_symbols: Set[str] = None
    years_examined: int = 5

    def set_defaults(self):
        self.ignored_date_strs = get_ignored_sp500_equity_dates()
        self.ignored_symbols = get_ignored_sp500_symbols()


def get_profile_misses(cfg: GetDatabaseMissesConfig) -> List[datetime]:
    missing_dt_lst = []

    for dt in cfg.datetimes:
        if not cfg.database.contains(cfg.symbol, dt):
            missing_dt_lst.append(dt)

    return missing_dt_lst


@dataclass
class GetProfilesConfig:
    symbol: str
    polygon_client: RESTClient
    datetimes: List[datetime]


def get_profiles(cfg: GetProfilesConfig) -> List[Tuple[datetime, object]]:
    profiles: List[Tuple[datetime, object]] = []
    for dt in cfg.datetimes:
        val = cfg.polygon_client.get_ticker_details(
            ticker=cfg.symbol, date=str(dt.date())
        )
        if val is None:
            _logger.warn(f"None when pulling profile for {dt} {cfg.symbol}")
            continue
        profiles.append(
            (dt, {"total_employees": val.total_employees, "market_cap": val.market_cap})
        )
    return profiles


@dataclass
class ExtractPriceConfig:
    database: DatabaseInterface
    target_datetime: datetime
    datetime_to_data: Dict[datetime, object]


@dataclass
class UpdateDatabaseConfig:
    symbol: str
    database: DatabaseInterface
    missing_datetimes: List[datetime]
    datetime_to_data: Dict[datetime, object]
    entry_type: str

    def to_extract_price_config(self, dt: datetime) -> ExtractPriceConfig:
        return ExtractPriceConfig(self.database, dt, self.datetime_to_data)


def fill_new_entries(
    cfg: UpdateDatabaseConfig,
) -> int:
    update_count = 0
    for idx, dt in enumerate(cfg.missing_datetimes):
        if cfg.database.contains_datetime(dt):
            if not cfg.database.contains_symbol(cfg.symbol):
                cfg.database.add_symbol(cfg.symbol, cfg.entry_type)

            profile = cfg.datetime_to_data[datetime_key(dt)]
            if profile is not None:
                cfg.database.add_entry(cfg.symbol, dt, profile)
                update_count += 1
    return update_count


# uses UpdateDatabaseConfig but actually does not need all its fields
def generate_new_rows(cfg: UpdateDatabaseConfig) -> List[Tuple[datetime, object]]:
    new_rows = []
    for idx, dt in enumerate(cfg.missing_datetimes):
        if not cfg.database.contains_datetime(dt):
            profile = cfg.datetime_to_data[datetime_key(dt)]
            if profile is not None:
                new_rows += [(dt, profile)]
    return new_rows
