from __future__ import annotations

import functools

from enum import Enum as BaseEnum
from enum import EnumMeta as BaseEnumMeta


class EnumMeta(BaseEnumMeta):
    def __getitem__(self, item):
        try:
            return self(item)
        except ValueError:
            return self._member_map_[item]

    def __contains__(self, item):
        if item in self._member_map_ or item in self._value2member_map_:
            return True
        return super().__contains__(item)


class Enum(BaseEnum, metaclass=EnumMeta):
    pass


# https://pandas.pydata.org/docs/user_guide/timeseries.html#period-aliases
class Freq(Enum):
    min_1 = "1min"
    min_5 = "5min"
    min_10 = "10min"
    min_15 = "15min"
    min_30 = "30min"
    hour = "h"
    day = "D"
    week = "W"
    month = "M"
    quarter = "Q"
    year = "Y"

    @functools.lru_cache()
    def __get_indexes(self, this, other):
        order = list(self._member_map_.keys())  # type: ignore
        return order.index(this.name), order.index(other.name)

    def __lt__(self, other):
        self_i, other_i = self.__get_indexes(self, other)
        return self_i < other_i

    def __le__(self, other):
        self_i, other_i = self.__get_indexes(self, other)
        return self_i <= other_i
