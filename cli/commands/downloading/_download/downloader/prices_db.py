from abc import abstractmethod
from datetime import datetime
from typing_extensions import Protocol

import pandas as pd


class PriceDatabaseInterface(Protocol):
    @abstractmethod
    def add_entry(self, symbol: str, dt: datetime, price: float) -> None:
        "add an entry for a price"

    @abstractmethod
    def add_rows(self, new_rows_df: pd.DataFrame) -> None:
        "add a set of rows to the db"

    @abstractmethod
    def add_symbol(self, symbol: str) -> None:
        "add a row for a certain symbol"

    @abstractmethod
    def contains(self, symbol: str, dt: datetime) -> bool:
        "determine if the db contains a valid entry for the symbol and datetime"

    @abstractmethod
    def contains_datetime(self, dt: datetime) -> bool:
        "determine if the db contains a valid entry for the datetime"

    @abstractmethod
    def contains_symbol(self, symbol: str) -> bool:
        "determine if the db contains a valid entry for the symbol"

    @abstractmethod
    def load(self, filepath: str) -> None:
        "load database from memory"


class DataframeDatabase(PriceDatabaseInterface):
    def __init__(self):
        self._df = pd.DataFrame()

    def add_entry(self, symbol: str, dt: datetime, price: float) -> None:
        self._df[symbol][dt] = price

    def add_rows(self, new_rows_df: pd.DataFrame) -> None:
        self._df = pd.concat([new_rows_df, self._df])

    def add_symbol(self, symbol: str) -> None:
        self._df[symbol] = pd.Series(dtype="float64")

    def contains(self, symbol: str, dtime: datetime) -> bool:
        return (
            symbol in self._df
            and dtime in self._df[symbol]
            and not pd.isna(self._df[symbol][dtime])
        )

    def contains_datetime(self, dtime: datetime) -> bool:
        return dtime in self._df.index

    def contains_symbol(self, symbol: str) -> bool:
        return symbol in self._df

    def load(self, filepath: str) -> None:
        self._df = pd.read_parquet(filepath)
