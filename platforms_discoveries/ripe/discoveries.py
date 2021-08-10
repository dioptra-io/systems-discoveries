import bz2
import json
import os

from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional


def extract(ripe_file: Path, timerange: Optional[DateTimeRange] = None):
    fd = bz2.BZ2File(ripe_file, "r")
    for line in fd.readlines():
        print(line)
        data = json.loads(line.strip())
        print(data)
        break

    nodes = set()
    links = set()

    fd.close()
    return nodes, links


def get_nodes_links(
    out_dir: Path, timerange: Optional[DateTimeRange] = None, processes: int = None
):
    ripe_files = os.listdir(out_dir)
    results = Pool(processes).map(
        partial(extract, timerange=timerange),
        [out_dir / ripe_file for ripe_file in ripe_files],
    )

    nodes = set()
    links = set()
    for nodes_warts, links_warts in results:
        nodes.update(nodes_warts)
        links.update(links_warts)

    return nodes, links
