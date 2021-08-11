import datetime
import os
import requests

from bs4 import BeautifulSoup
from datetimerange import DateTimeRange
from multiprocessing import Pool
from pathlib import Path
from typing import Optional


def _download_file(current_date_formated, path_filename):
    r = requests.get(
        f"https://data-store.ripe.net/datasets/atlas-daily-dumps/"
        f"{current_date_formated}/{path_filename.name}",
    )

    with path_filename.open("wb") as fd:
        fd.write(r.content)


def filename_to_datetime(filename):
    datehour = filename[11:-4]
    d, h = datehour.split("T")
    t = h[:2] + ":00:00"
    return datetime.datetime.fromisoformat(d + "T" + t) + datetime.timedelta(hours=2)


def compute_probing_dates(time_range):
    dates = set()
    for hours in time_range.range(datetime.timedelta(hours=1)):
        dates.add(hours.date())
    dates.add(min(dates) - datetime.timedelta(days=1))
    return dates


def download_dataset(
    out_dir: Path,
    time_range: Optional[DateTimeRange] = None,
    processes: int = None,
):

    # To get the first hour
    extended_time_range = DateTimeRange(
        time_range.start_datetime - datetime.timedelta(hours=1), time_range.end_datetime
    )

    files_to_download = set()
    for probing_date in compute_probing_dates(extended_time_range):
        probing_date_formated = probing_date.isoformat()

        # Get the file list
        response = requests.get(
            "https://data-store.ripe.net/datasets/atlas-daily-dumps/"
            f"{probing_date_formated}"
        )

        filenames = []
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all("a", href=True):
            filename = a["href"]
            if filename.startswith("traceroute-"):
                filenames.append(filename)

        # Create a date local directory
        try:
            os.mkdir(out_dir)
        except FileExistsError:
            pass

        for filename in filenames:
            path_filename = out_dir / filename
            if filename_to_datetime(filename) not in extended_time_range:
                # Filter to keep the files only in the time range
                # The top of the hour must be in the timerange to be counted
                continue
            if path_filename.exists():
                continue

            files_to_download.add((probing_date_formated, path_filename))

    Pool(processes).starmap(_download_file, files_to_download)
