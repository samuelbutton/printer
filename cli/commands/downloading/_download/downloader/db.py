from abc import abstractmethod
from datetime import datetime
from typing import List, Tuple
from typing_extensions import Protocol

import pandas as pd


class DatabaseInterface(Protocol):
    @abstractmethod
    def add_entry(self, symbol: str, dt: datetime, entry: object) -> None:
        "add an entry"

    @abstractmethod
    def add_rows(self, symbol: str, new_rows: List[Tuple[datetime, object]]) -> None:
        "add a set of rows to the db"

    @abstractmethod
    def add_symbol(self, symbol: str, entry_type: str) -> None:
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

    @abstractmethod
    def save(self, filepath: str) -> None:
        "save database to memory"


class DataframeDatabase(DatabaseInterface):
    def __init__(self):
        self._df = pd.DataFrame()

    def add_entry(self, symbol: str, dt: datetime, entry: object) -> None:
        if entry is None:
            return
        self._df[symbol][dt] = entry

    def add_rows(self, symbol: str, new_rows: List[Tuple[datetime, object]]) -> None:
        self._df = pd.concat([to_df(symbol, new_rows), self._df])

    def add_symbol(self, symbol: str, entry_type: str) -> None:
        self._df[symbol] = pd.Series(dtype=entry_type)

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

    def save(self, filepath: str) -> None:
        self._df.to_parquet(filepath)


def to_df(symbol: str, datetime_list: List[Tuple[datetime, object]]) -> pd.DataFrame:
    df = pd.DataFrame()
    for dt, obj in datetime_list:
        df = pd.concat(
            [df, pd.DataFrame({symbol: {dt: obj}})],
            ignore_index=False,
        )
    return df
