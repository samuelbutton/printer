from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from os.path import exists
from typing import Dict, List, Optional, Set, Tuple

from alpaca.data import (
    StockBarsRequest,
    StockHistoricalDataClient,
    TimeFrame,
    TimeFrameUnit,
)
import pandas as pd
from pandas_market_calendars.calendar_registry import MarketCalendar
import pytz

from ..configs.download import DownloadConfig
from .downloader import Downloader
from .db import DatabaseInterface, DataframeDatabase
from .utils import (
    LoadMarketCalendarConfig,
    load_market_calendar,
    to_datetime_mapping,
)

_logger = logging.getLogger(__name__)


class PricesDownloader(Downloader):
    def __init__(self, cfg: DownloadConfig):
        self.cfg: DownloadConfig = cfg
        if cfg.symbols_limit is not None:
            self.cfg.symbols = self.cfg.symbols[: cfg.symbols_limit]

        self._alpaca_client: Optional[StockHistoricalDataClient] = None

        self._database: Optional[DatabaseInterface] = None
        if cfg.use_existing_db and exists(cfg.database_filepath):
            _logger.info("loading existing database")
            self._database = DataframeDatabase()
            self._database.load(cfg.database_filepath)

        self.market_calendar = load_market_calendar(
            LoadMarketCalendarConfig(
                start_date=str(
                    datetime.now() - timedelta(days=round(cfg.years_examined * 365))
                ),
                end_date=str(datetime.now() - timedelta(days=1)),
            ),
        )

    @property
    def alpaca_client(self) -> StockHistoricalDataClient:
        if self._alpaca_client is None:
            _logger.info("loading alpaca client")
            self._alpaca_client = StockHistoricalDataClient(
                api_key=self.cfg.alpaca_key_id,
                secret_key=self.cfg.alpaca_secret_key,
            )
        return self._alpaca_client

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
            market_calendar=self.market_calendar,
        )
        cfg.set_defaults()
        return get_price_misses(cfg)

    def pull_missing_data(
        self, symbol: str, missing_datetimes: List[datetime]
    ) -> List[Tuple[datetime, object]]:
        _logger.debug(f"Pulling missing data: {symbol}")
        return get_prices(
            GetPricesConfig(
                symbol,
                self.alpaca_client,
                missing_datetimes[0],
                missing_datetimes[-1],
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

        dt_to_price = to_datetime_mapping(pulled_data)

        cfg = UpdateDatabaseConfig(
            symbol,
            self.database,
            missing_datetimes,
            dt_to_price,
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
    market_calendar: MarketCalendar
    ignored_date_strs: Set[str] = None
    ignored_symbols: Set[str] = None
    evaluation_hours: int = 6.5
    record_frequency_minutes: int = 12
    start_evaluation_time_str: str = "09:30:00"
    timezone: pytz.timezone = pytz.timezone("US/Eastern")
    years_examined: int = 5

    def set_defaults(self):
        self.ignored_date_strs = get_ignored_sp500_equity_dates()
        self.ignored_symbols = get_ignored_sp500_symbols()


def get_price_misses(cfg: GetDatabaseMissesConfig) -> List[datetime]:
    missing_dt_lst = []

    for opening_dt in cfg.market_calendar:
        base_dt = cfg.timezone.localize(
            datetime.strptime(
                f"{opening_dt.date()} {cfg.start_evaluation_time_str}",
                "%Y-%m-%d %H:%M:%S",
            )
        )
        for i in range(round(cfg.evaluation_hours * cfg.record_frequency_minutes) + 1):
            loop_dt = base_dt + timedelta(minutes=i * 60 / cfg.record_frequency_minutes)
            if (
                cfg.symbol not in cfg.ignored_symbols
                and str(loop_dt.date()) not in cfg.ignored_date_strs
                and not cfg.database.contains(cfg.symbol, loop_dt)
            ):
                missing_dt_lst.append(loop_dt)

    return missing_dt_lst


@dataclass
class GetPricesConfig:
    symbol: str
    stock_historical_data_client: StockHistoricalDataClient
    start_datetime: datetime
    end_datetime: datetime
    time_frame_amount: int = 1
    time_frame_unit: TimeFrameUnit = TimeFrameUnit("Min")


def get_prices(cfg: GetPricesConfig) -> List[Tuple[datetime, object]]:
    try:
        return [
            (entry["timestamp"], entry)
            for entry in cfg.stock_historical_data_client.get_stock_bars(
                StockBarsRequest(
                    symbol_or_symbols=cfg.symbol,
                    start=cfg.start_datetime,
                    end=cfg.end_datetime,
                    timeframe=TimeFrame(
                        amount=cfg.time_frame_amount,
                        unit=cfg.time_frame_unit,
                    ),
                )
            ).dict()[cfg.symbol]
        ]
    except AttributeError as e:
        _logger.warning(f"get_prices for {cfg.symbol} was not successful", e)
    return None


@dataclass
class ExtractPriceConfig:
    database: DatabaseInterface
    target_datetime: datetime
    datetime_to_data: Dict[datetime, object]


def extract_price(cfg: ExtractPriceConfig) -> float:
    price = 0
    for i in [0, 1, -1, 2, -2, 3, -3, 4, -4]:
        new_dt = cfg.target_datetime + timedelta(minutes=i)
        key = datetime_key(new_dt)
        if key in cfg.datetime_to_data:
            return cfg.datetime_to_data[key]["close"]
    return None


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

            price = extract_price(cfg.to_extract_price_config(dt))
            if price is not None:
                cfg.database.add_entry(cfg.symbol, dt, price)
                update_count += 1
    return update_count


# uses UpdateDatabaseConfig but actually does not need all its fields
def generate_new_rows(cfg: UpdateDatabaseConfig) -> List[Tuple[datetime, object]]:
    new_rows = []
    for idx, dt in enumerate(cfg.missing_datetimes):
        if not cfg.database.contains_datetime(dt):
            price = extract_price(cfg.to_extract_price_config(dt))
            if price is not None:
                new_rows += [(dt, price)]
    return new_rows
