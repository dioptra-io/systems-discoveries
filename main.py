import sys

from platforms_discoveries.ark.download import download_dataset as ark_dataset
from platforms_discoveries.ark.discoveries import get_nodes_links as ark_nodes_links
from platforms_discoveries.ripe.download import download_dataset as ripe_dataset
from platforms_discoveries.ripe.discoveries import get_nodes_links as ripe_nodes_links


from datetime import date
from datetime import datetime
from datetimerange import DateTimeRange
from pathlib import Path


def ark():
    """Needs credentials because up-to-date data are restricted."""
    credentials = sys.argv[1]
    probing_date = date.fromisoformat("2021-08-06")
    ark_dir = Path("./data/ark") / probing_date.strftime("%Y%m%d")
    timerange = (
        DateTimeRange(
            datetime.fromisoformat("2021-08-06T11:59:59"),
            datetime.fromisoformat("2021-08-06T16:35:31"),
        ),
    )

    # Download warts files
    ark_dataset(probing_date, ark_dir, credentials, timerange=timerange)

    # Compute nodes and links
    nodes, links = ark_nodes_links(ark_dir, timerange=timerange)

    print(len(nodes), len(links))


def ripe():
    """Everything is public data."""
    probing_date = date.fromisoformat("2021-08-06")
    ripe_dir = Path("./data/ripe") / probing_date.strftime("%Y%m%d")
    timerange = (
        DateTimeRange(
            datetime.fromisoformat("2021-08-06T11:59:59"),
            datetime.fromisoformat("2021-08-06T16:35:31"),
        ),
    )

    # Download RIPE files
    ripe_dataset(probing_date, ripe_dir, timerange=timerange)

    # Compute nodes and links
    nodes, links = ripe_nodes_links(ripe_dir, timerange=timerange)

    print(len(nodes), len(links))


if __name__ == "__main__":
    ripe()
