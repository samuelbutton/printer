from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import List, Optional, Set, Tuple

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
from .prices_db import PriceDatabaseInterface, DataframeDatabase
from .utils import LoadMarketCalendarConfig, load_market_calendar

_logger = logging.getLogger(__name__)


class PricesDownloader(Downloader):
    def __init__(self, cfg: DownloadConfig):
        self.cfg: DownloadConfig = cfg
        if cfg.symbols_limit is not None:
            self.cfg.symbols = self.cfg.symbols[: cfg.symbols_limit]

        self._alpaca_client: Optional[StockHistoricalDataClient] = None

        self._price_database: Optional[PriceDatabaseInterface] = None
        if cfg.use_existing_db:
            _logger.info("loading existing database")
            self._price_database = DataframeDatabase()
            self._price_database.load(cfg.database_filepath)

        self.market_calendar = load_market_calendar(
            LoadMarketCalendarConfig(
                # 5 years
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
    def price_database(self) -> PriceDatabaseInterface:
        if self._price_database is None:
            _logger.info("loading new price database")
            self._price_database = DataframeDatabase()
        return self._price_database

    def save_to_db(self):
        _logger.info("saving db!")

    def find_missing_data(self) -> List[Tuple[(str, List[datetime.date])]]:
        _logger.info("Finding missing data...")

        all_missing_dts: List[Tuple[(str, List[datetime.date])]] = []
        for i, symbol in enumerate(self.cfg.symbols):
            _logger.debug(f"Finding missing data for {symbol}")
            missing_dts = self._get_misses(symbol)
            if len(missing_dts) == 0:
                continue
            all_missing_dts.append((symbol, missing_dts))

        return all_missing_dts

    def pull_missing_data(
        self, symbol: str, missing_datetimes: List[datetime]
    ) -> pd.DataFrame:
        _logger.debug(f"Pulling prices: {symbol}")
        return get_prices(
            GetPricesConfig(
                symbol,
                self.alpaca_client,
                missing_datetimes[0],
                missing_datetimes[-1],
            )
        )

    def save_to_database(
        self, symbol: str, missing_datetimes: List[datetime], missing_data: pd.DataFrame
    ) -> None:
        _logger.debug(f"Saving to database: {symbol}")
        if missing_data is None or len(missing_datetimes) == 0 or len(missing_data) < 0:
            return
        new_rows_df = update_prices(
            UpdatePricesConfig(
                symbol, self.price_database, missing_datetimes, missing_data
            )
        )
        self.price_database.add_rows(new_rows_df)
        # self.price_db.to_parquet(price_db_fn)

    def _get_misses(self, symbol: str) -> List[datetime.date]:
        # use all the defaults
        cfg = GetPriceDatabaseMissesConfig(
            symbol=symbol,
            price_database=self.price_database,
            market_calendar=self.market_calendar,
        )
        cfg.set_defaults()
        return get_price_misses(cfg)


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
        ]
    )


@dataclass
class GetPriceDatabaseMissesConfig:
    symbol: str
    price_database: PriceDatabaseInterface
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


def get_price_misses(cfg: GetPriceDatabaseMissesConfig) -> List[datetime]:
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
            if str(
                loop_dt.date()
            ) not in cfg.ignored_date_strs and not cfg.price_database.contains(
                cfg.symbol, loop_dt
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


def get_prices(cfg: GetPricesConfig) -> pd.DataFrame:
    try:
        return cfg.stock_historical_data_client.get_stock_bars(
            StockBarsRequest(
                symbol_or_symbols=cfg.symbol,
                start=cfg.start_datetime,
                end=cfg.end_datetime,
                timeframe=TimeFrame(
                    amount=cfg.time_frame_amount,
                    unit=cfg.time_frame_unit,
                ),
            )
        ).df.reset_index("symbol")
    except AttributeError as e:
        _logger.warning(f"get_prices for {cfg.symbol} was not successful", e)
    return None


@dataclass
class UpdatePricesConfig:
    symbol: str
    price_database: PriceDatabaseInterface
    missing_datetimes: List[datetime]
    missing_prices: pd.DataFrame


def update_prices(cfg: UpdatePricesConfig) -> pd.DataFrame:
    new_rows = pd.DataFrame()
    price_miss_count = 0
    for idx, dt in enumerate(cfg.missing_datetimes):
        price = 0
        # for a 5 minute buffer...
        for i in range(5):
            # check step up
            new_dt = dt + timedelta(minutes=i)
            if len(cfg.missing_prices[new_dt:new_dt]["close"]) == 1:
                price = cfg.missing_prices[new_dt:new_dt]["close"][0]
                break

            # check step down
            new_dt = dt - timedelta(minutes=i)
            if len(cfg.missing_prices[new_dt:new_dt]["close"]) == 1:
                price = cfg.missing_prices[new_dt:new_dt]["close"][0]
                break

        if price == 0:
            price_miss_count += 1
            continue

        if not cfg.price_database.contains_datetime(dt):
            new_rows = pd.concat(
                [new_rows, pd.DataFrame({cfg.symbol: {dt: price}})],
                ignore_index=False,
            )
        else:
            if not cfg.price_database.contains_symbol(cfg.symbol):
                cfg.price_database.add_symbol(cfg.symbol)
            cfg.price_database.add_entry(cfg.symbol, dt, price)

    if price_miss_count > 0:
        _logger.warn(f"price_miss_count {price_miss_count}")

    return new_rows
