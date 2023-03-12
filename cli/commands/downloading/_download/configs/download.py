from dataclasses import dataclass
from enum import Enum
import os


import pytz
from typing import Set, List
import pandas as pd


class DownloaderEnum(Enum):
    PricesDownloader = "PricesDownloader"
    ProfilesDownloader = "ProfilesDownloader"
    FinancialsDownloader = "FinancialsDownloader"


@dataclass
class DownloadConfig:
    database_filepath: str
    downloader_enum: DownloaderEnum
    symbols: List[str]
    use_existing_db: bool
    years_examined: int
    database_entry_type: str
    symbols_limit: int = None
    alpaca_key_id: str = None
    alpaca_secret_key: str = None
    polygon_api_key: str = None


CONFIG_CHOICES = {
    "sp500_equity_prices": DownloadConfig(
        alpaca_key_id=os.environ.get("ALYOSHA_ALPACA_API_KEY_ID"),
        alpaca_secret_key=os.environ.get("ALYOSHA_ALPACA_API_SECRET"),
        database_filepath="./data/sp500_equity_prices.parquet",
        downloader_enum=DownloaderEnum.PricesDownloader,
        symbols=pd.read_csv("./data/sp500_equity_symbols.csv")["Symbol"],
        use_existing_db=True,
        years_examined=5,
        symbols_limit=10,
        database_entry_type="float64",
    ),
    "sp500_equity_profiles": DownloadConfig(
        polygon_api_key=os.environ.get("POLYGON_API_KEY"),
        database_filepath="./data/sp500_equity_profiles.parquet",
        downloader_enum=DownloaderEnum.ProfilesDownloader,
        symbols=pd.read_csv("./data/sp500_equity_symbols.csv")["Symbol"],
        use_existing_db=True,
        years_examined=5,
        symbols_limit=5,
        database_entry_type="object",
    ),
    "sp500_equity_financials": DownloadConfig(
        polygon_api_key=os.environ.get("POLYGON_API_KEY"),
        database_filepath="./data/sp500_equity_financials.parquet",
        downloader_enum=DownloaderEnum.FinancialsDownloader,
        symbols=pd.read_csv("./data/sp500_equity_symbols.csv")["Symbol"],
        use_existing_db=True,
        years_examined=5,
        symbols_limit=5,
        database_entry_type="object",
    ),
}
