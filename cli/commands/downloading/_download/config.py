from dataclasses import dataclass

import pytz
from typing import Set

from core.models import CandidateGroup


@dataclass
class DownloadConfig:
    # no default
    db_filename: str
    ignored_symbols: Set[str]
    ignored_date_strs: Set[str]

    # maybe only specific to equities
    record_frequency_minutes: int = 12
    start_evaluation_time_str: str = "09:30:00"
    evaluation_hours: int = 6.5
    timezone: pytz.timezone = pytz.timezone("US/Eastern")

    # defaults
    candidate_group: CandidateGroup = CandidateGroup.sp500
    use_existing_db: bool = True
    years_examined: int = 5


def get_ignored_sp500_equity_symbols():
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


def get_ignored_sp500_equity_dates():
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


def get_config(
    db_filename: str,
) -> DownloadConfig:
    return DownloadConfig(
        db_filename=db_filename,
        ignored_symbols=get_ignored_sp500_equity_symbols(),
        ignored_date_strs=get_ignored_sp500_equity_dates(),
    )


CONFIG_CHOICES = {
    "sp500_equity_prices": get_config(db_filename="sp500_equity_prices.parquet"),
    "sp500_equity_profiles": get_config(db_filename="sp500_equity_profiles.parquet"),
    "sp500_equity_financials": get_config(
        db_filename="sp500_equity_financials.parquet"
    ),
}
