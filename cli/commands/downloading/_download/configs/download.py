from dataclasses import dataclass
from enum import Enum
import os


import pytz
from typing import Set, List
import pandas as pd


class DownloaderEnum(Enum):
    PricesDownloader = "PricesDownloader"
    # ProfilesDownloader="ProfilesDownloader"
    # FinancialsDownloader="FinancialsDownloader"


@dataclass
class DownloadConfig:
    alpaca_key_id: str
    alpaca_secret_key: str
    database_filepath: str
    downloader_enum: DownloaderEnum
    symbols: List[str]
    use_existing_db: bool
    years_examined: int
    symbols_limit: int = None


CONFIG_CHOICES = {
    "sp500_equity_prices": DownloadConfig(
        alpaca_key_id=os.environ.get("ALYOSHA_ALPACA_API_KEY_ID"),
        alpaca_secret_key=os.environ.get("ALYOSHA_ALPACA_API_SECRET"),
        database_filepath="./data/sp500_equity_prices.parquet",
        downloader_enum=DownloaderEnum.PricesDownloader,
        symbols=pd.read_csv("./data/sp500_equity_symbols.csv")["Symbol"],
        use_existing_db=True,
        years_examined=5,
        # setting for debugging
        symbols_limit=5,
    ),
    # "sp500_equity_profiles":
    # "sp500_equity_financials":
}
