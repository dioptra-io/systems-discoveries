import datetime
import os
import requests

from bs4 import BeautifulSoup
from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional


def _download_file(probing_date, path_filename):
    r = requests.get(
        f"https://data-store.ripe.net/datasets/atlas-daily-dumps/{probing_date}/"
        f"{path_filename.name}",
    )

    with path_filename.open("wb") as fd:
        fd.write(r.content)


def filename_to_datetime(filename):
    datehour = filename[11:-4]
    d, h = datehour.split("T")
    t = h[:2] + ":00:00"
    return datetime.datetime.fromisoformat(d + "T" + t)


def download_dataset(
    probing_date: datetime.date,
    out_dir: Path,
    timerange: Optional[DateTimeRange] = None,
    processes: int = None,
):

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

    files_to_download = set()
    for filename in filenames:
        path_filename = out_dir / filename
        if timerange and filename_to_datetime(filename) not in timerange:
            # Filter to keep the files only in the timerange
            # The top of the hour must be in the timerange to be counted
            continue
        if path_filename.exists():
            continue

        files_to_download.add(path_filename)

    return

    Pool(processes).map(
        partial(_download_file, probing_date_formated), files_to_download
    )
