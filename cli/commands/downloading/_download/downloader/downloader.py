from typing import List, Tuple
from datetime import datetime

import pandas as pd


class Downloader:
    def find_missing_dates(self, symbol: str) -> List[datetime.date]:
        raise NotImplementedError

    def pull_missing_data(
        self, symbol: str, missing_datetimes: List[datetime]
    ) -> List[Tuple[datetime, object]]:
        raise NotImplementedError

    def save_to_database(
        self,
        symbol: str,
        missing_datetimes: List[datetime],
        pulled_data: List[Tuple[datetime, object]],
    ) -> None:
        raise NotImplementedError

    def symbols(self) -> List[str]:
        raise NotImplementedError
