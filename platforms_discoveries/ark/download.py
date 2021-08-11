import datetime
import requests

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


def compute_probing_dates(time_range):
    dates = set()
    for hours in time_range.range(datetime.timedelta(hours=1)):
        dates.add(hours.date())
    dates.add(min(dates) - datetime.timedelta(days=1))
    return dates


def download_dataset(
    out_dir: Path,
    credentials: str,
    time_range: Optional[DateTimeRange] = None,
    processes: int = None,
):
    userAndPass = b64encode(str.encode(credentials)).decode("ascii")
    headers = {"Authorization": "Basic %s" % userAndPass}

    files_to_download = set()
    for probing_date in compute_probing_dates(time_range):
        probing_date_formated = probing_date.strftime("%Y%m%d")

        # Get the warts list
        response = requests.get(
            "https://data.caida.org/datasets/topology/ark/ipv4/probe-data/team-1/daily/"
            f"{probing_date_formated[:4]}/cycle-{probing_date_formated}/",
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

            files_to_download.add((probing_date_formated, path_filename))

    Pool(processes).starmap(partial(_download_wart, headers), files_to_download)
