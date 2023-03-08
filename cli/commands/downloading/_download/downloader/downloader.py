from typing import List
from datetime import datetime

import pandas as pd


class Downloader:
    def find_missing_data(self):
        raise NotImplementedError

    def pull_missing_data(
        self, symbol: str, missing_datetimes: List[datetime]
    ) -> pd.DataFrame:
        raise NotImplementedError

    def save_to_database(
        self, symbol: str, missing_datetimes: List[datetime], missing_data: pd.DataFrame
    ) -> None:
        raise NotImplementedError
