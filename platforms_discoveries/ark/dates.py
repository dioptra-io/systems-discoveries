import datetime
from datetimerange import DateTimeRange
from typing import List


def probing_dates_from_time_range(time_range: DateTimeRange) -> List[datetime.datetime]:
    """Get the list of `datetime.datetime` from a time range."""
    dates = set()
    for hours in time_range.range(datetime.timedelta(hours=1)):
        dates.add(hours.date())
    dates.add(min(dates) - datetime.timedelta(days=1))
    return dates


def filename_to_datetime(filename: str) -> datetime.datetime:
    """Get the list of `datetime.datetime` from a time range."""
    formated_date = filename.split(".")[3]
    d = datetime.date(
        int(formated_date[:4]), int(formated_date[4:6]), int(formated_date[6:])
    )
    return datetime.datetime.combine(d, datetime.datetime.min.time())


def is_filename_time_range(filename: str, time_range: DateTimeRange) -> bool:
    """Check if a filename is in a time range"""
    probing_dates = probing_dates_from_time_range(time_range)
    probing_dates_formated = [
        probing_date.strftime("%Y%m%d") for probing_date in probing_dates
    ]
    formated_date = filename.split(".")[3]
    return formated_date in probing_dates_formated
