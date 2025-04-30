

from datetime import date, datetime
from typing import Union


def parse_int(s: Union[str, None]) -> Union[int, None]:
    if not s:
        return None
    else:
        return int(s)


def parse_float(s: Union[str, None]) -> Union[float, None]:
    if not s:
        return None
    else:
        return float(s)

def parse_float_div_by(s: Union[str, None], div_by: float) -> Union[float, None]:
    if not s:
        return None
    else:
        return float(s) / div_by

def parse_date_ymd(s: Union[str, None]) -> Union[date, None]:
    if not s:
        return None
    elif len(s) == 8:
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    elif len(s) == 10:
        return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
    else:
        raise ValueError("Unexpected string length for date " + s)

def parse_datetime_iso(s: Union[str, None]) -> Union[datetime, None]:
    if not s:
        return None
    else:
        return datetime.fromisoformat(s)