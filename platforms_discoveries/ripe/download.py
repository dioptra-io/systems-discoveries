import requests

from bs4 import BeautifulSoup
from datetimerange import DateTimeRange
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

from platforms_discoveries.ripe.dates import (
    probing_dates_from_time_range,
    is_filename_time_range,
)


def _download_file(current_date_formated, path_filename):
    r = requests.get(
        f"https://data-store.ripe.net/datasets/atlas-daily-dumps/"
        f"{current_date_formated}/{path_filename.name}",
    )

    with path_filename.open("wb") as fd:
        fd.write(r.content)


def download_dataset(
    out_dir: Path,
    time_range: Optional[DateTimeRange] = None,
    processes: int = None,
):
    files_to_download = set()
    for probing_date in probing_dates_from_time_range(time_range):
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

        for filename in filenames:
            path_filename = out_dir / filename
            if not is_filename_time_range(filename, time_range):
                # Filter to keep the files only in the time range
                continue
            if path_filename.exists():
                continue

            files_to_download.add((probing_date_formated, path_filename))

    Pool(processes).starmap(_download_file, files_to_download)
