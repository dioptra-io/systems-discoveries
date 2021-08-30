import bz2
import json
import os

from datetime import datetime
from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional

from platforms_discoveries.ripe.dates import filename_to_datetime


def extract(time_range: Optional[DateTimeRange], ripe_file: Path):
    fd = bz2.BZ2File(ripe_file, "r")

    nodes = set()
    links = set()

    n_traceroutes = 0
    n_probes = 0
    dst_prefixes = set()

    for line in fd:
        data = json.loads(line.strip())

        # Discard None data
        if data is None:
            continue

        # Discard Data not in time range if any
        if datetime.fromtimestamp(data["timestamp"]) not in time_range:
            continue

        # Only include IPv4
        if not data["af"] == 4:
            continue

        n_traceroutes += 1
        try:
            dst_prefixes.add(".".join(data["dst_addr"].split(".")[:3] + ["0"]))
        except KeyError:
            pass

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
                n_probes += 1
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
                last_hop = None
                last_node = None

    fd.close()
    return nodes, links, n_traceroutes, n_probes, dst_prefixes


def get_nodes_links(
    out_dir: Path, time_range: Optional[DateTimeRange] = None, processes: int = None
):
    all_files = os.listdir(out_dir)
    ripe_files = []
    for filename in all_files:
        if filename_to_datetime(filename) not in time_range:
            continue
        ripe_files.append(filename)

    results = Pool(processes).map(
        partial(extract, time_range),
        [out_dir / ripe_file for ripe_file in ripe_files],
    )

    nodes = set()
    links = set()

    global_n_traceroutes = 0
    global_n_probes = 0
    global_prefixes = set()

    for nodes_file, links_file, n_traceroutes, n_probes, prefixes in results:
        nodes.update(nodes_file)
        links.update(links_file)

        global_n_traceroutes += n_traceroutes
        global_n_probes += n_probes
        global_prefixes.update(prefixes)

    print("Number of traceroutes:", global_n_traceroutes)
    print("Number of probes:", global_n_probes)
    print("Number of prefixes:", len(global_prefixes))

    return nodes, links
