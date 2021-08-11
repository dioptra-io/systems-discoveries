import bz2
import json
import os

from datetime import datetime
from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional


def extract(ripe_file: Path, timerange: Optional[DateTimeRange] = None):
    fd = bz2.BZ2File(ripe_file, "r")

    nodes = set()
    links = set()

    per_packet = 0
    for line in fd:
        data = json.loads(line.strip())

        # Discard None data
        if data is None:
            continue

        # Discard Data not in timerange if any
        if timerange and datetime.fromtimestamp(data["timestamp"]) not in timerange:
            continue

        # Only include IPv4
        if not data["af"] == 4:
            continue

        # Compute the nodes and links
        last_hop = None
        last_node = None
        for result in data["result"]:
            hop = result.get("hop")
            replies = result.get("result")

            if not hop or not replies:
                last_hop = None
                last_node = None
                continue

            node = set()
            for reply in replies:
                n = reply.get("from")
                if n:
                    node.add(n)

            if len(node) == 1:
                node = next(iter(node))
                nodes.add(node)
                if (
                    last_hop is not None
                    and last_node is not None
                    and last_hop + 1 == hop
                ):
                    links.add((last_node, node))
                last_hop = hop
                last_node = node
            elif len(node) > 1:
                # per-packet load balancing
                per_packet += 1
                break

    print("per packet", per_packet)
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
