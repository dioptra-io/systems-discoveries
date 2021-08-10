import datetime
import requests
import os

from base64 import b64encode
from bs4 import BeautifulSoup
from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional


def _download_wart(headers, current_date_formated, path_filename):
    r = requests.get(
        "https://data.caida.org/datasets"
        "/topology/ark/ipv4/probe-data/team-1/"
        f"daily/{current_date_formated[:4]}/"
        f"cycle-{current_date_formated}/{path_filename.name}",
        headers=headers,
    )

    with path_filename.open("wb") as fd:
        fd.write(r.content)


def download_dataset(
    probing_date: datetime.date,
    out_dir: Path,
    credentials: str,
    timerange: Optional[DateTimeRange] = None,
    processes: int = None,
):
    userAndPass = b64encode(str.encode(credentials)).decode("ascii")
    headers = {"Authorization": "Basic %s" % userAndPass}

    probing_date_formated = probing_date.strftime("%Y%m%d")

    # Create a date local directory
    try:
        os.mkdir(out_dir)
    except FileExistsError:
        pass

    files_to_download = set()
    for offset in [-1, 0, +1]:
        # Compute the correct day
        current_date = probing_date + datetime.timedelta(days=offset)
        current_date_formated = current_date.strftime("%Y%m%d")

        # Get the warts list
        response = requests.get(
            "https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily/"
            f"{current_date_formated[:4]}/cycle-{current_date_formated}/",
            headers=headers,
        )

        filenames = []
        soup = BeautifulSoup(response.text, "html.parser")
        for a in soup.find_all("a", href=True):
            filename = a["href"]
            if "warts.gz" in filename:
                filenames.append(filename)

        # Download files with the correct date
        for filename in filenames:
            path_filename = out_dir / filename
            if probing_date_formated not in filename:
                continue
            if path_filename.exists():
                continue

            files_to_download.add((current_date_formated, path_filename))

    Pool(processes).starmap(partial(_download_wart, headers), files_to_download)
