import sys

from platforms_discoveries.ark.download import download_warts
from platforms_discoveries.ark.discoveries import get_nodes_links

from datetime import date
from datetime import datetime
from datetimerange import DateTimeRange
from pathlib import Path


def ark():
    credentials = sys.argv[1]
    probing_date = date.fromisoformat("2021-08-06")
    wart_dir = Path("./data/ark") / probing_date.strftime("%Y%m%d")

    # Download warts files
    download_warts(probing_date, wart_dir, credentials)

    # Compute nodes and links
    nodes, links = get_nodes_links(
        wart_dir,
        timerange=DateTimeRange(
            datetime.fromisoformat("2021-08-06T12:01:50"),
            datetime.fromisoformat("2021-08-06T16:35:31"),
        ),
    )

    print(len(nodes), len(links))


if __name__ == "__main__":
    ark()
