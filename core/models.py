from enum import Enum


class CandidateGroup(Enum):
    sp500 = "sp500"

    def __str__(self) -> str:
        return self.value
