import datetime
from datetimerange import DateTimeRange
from typing import List


def probing_dates_from_time_range(time_range: DateTimeRange) -> List[datetime.datetime]:
    """Get the list of `datetime.datetime` from a time range."""
    # To get the first hour
    extended_time_range = DateTimeRange(
        time_range.start_datetime - datetime.timedelta(hours=1), time_range.end_datetime
    )

    dates = set()
    for hours in extended_time_range.range(datetime.timedelta(hours=1)):
        dates.add(hours.date())
    dates.add(min(dates) - datetime.timedelta(days=1))
    return dates


def filename_to_datetime(filename: str) -> datetime.datetime:
    """Get the `datetime.datetime` from a filename."""
    datehour = filename[11:-4]
    d, h = datehour.split("T")
    t = h[:2] + ":00:00"
    return datetime.datetime.fromisoformat(d + "T" + t) + datetime.timedelta(hours=2)


def is_filename_time_range(filename: str, time_range: DateTimeRange) -> bool:
    """Check if a filename is in a time range"""
    # To get the first hour
    extended_time_range = DateTimeRange(
        time_range.start_datetime - datetime.timedelta(hours=1), time_range.end_datetime
    )
    return filename_to_datetime(filename) in extended_time_range
