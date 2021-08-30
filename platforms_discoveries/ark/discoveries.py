import bz2
import gzip
import os
import warts

from datetime import datetime
from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import List, Optional


from platforms_discoveries.ark.dates import is_filename_time_range


def warts_open(infile):
    fd = None
    # try reading as a bz2 file
    try:
        fd = bz2.BZ2File(infile, "rb")
        fd.read(1)
        fd = bz2.BZ2File(infile, "rb")
        return fd
    except IOError:
        pass
    # try reading as a gzip file
    try:
        fd = gzip.open(infile, "rb")
        fd.read(1)
        fd = gzip.open(infile, "rb")
        return fd
    except IOError:
        pass
    fd = open(infile, "rb")
    return fd


def decode_type_code(typecode):
    return [x for x in (typecode).to_bytes(2, byteorder="big")]


def check(record, hop):
    icmp_type, _ = decode_type_code(hop.reply_icmp_typecode)
    return hop.address != record.dst_address and icmp_type == 11


def extract(time_range: Optional[DateTimeRange], wart_file: Path):
    fd = warts_open(str(wart_file))

    nodes = set()
    links = set()

    min_timestamp = None
    max_timestamp = None

    n_traceroutes = 0
    n_probes = 0

    while True:
        try:
            record = warts.parse_record(fd)
        except Exception:
            print(f"ERROR EOF : {wart_file}")
            break
        if record is None:
            break
        if not isinstance(record, warts.traceroute.Traceroute):
            continue

        # In the case of cycles specification
        t = datetime.fromtimestamp(record.start_time)
        if not min_timestamp or t < min_timestamp:
            min_timestamp = t
        if not max_timestamp or t > max_timestamp:
            max_timestamp = t

        if datetime.fromtimestamp(record.start_time) not in time_range:
            continue

        n_traceroutes += 1
        if record.hops:
            n_probes += record.hops[-1].probe_ttl

        for i, hop in enumerate(record.hops[:-1]):
            if check(record, hop):
                # Node if not destination address
                nodes.add(hop.address)

                if hop.probe_ttl + 1 == record.hops[i + 1].probe_ttl:
                    # Add link
                    if check(record, record.hops[i + 1]):
                        links.add((hop.address, record.hops[i + 1].address))

        try:
            hop = record.hops[-1]
        except IndexError:
            continue

        if check(record, hop):
            # Get last nodes if not destination address
            nodes.add(hop.address)

    fd.close()
    return nodes, links, min_timestamp, max_timestamp, n_traceroutes, n_probes


def get_nodes_links(
    out_dir: Path,
    time_range: Optional[DateTimeRange] = None,
    cycles: Optional[List[str]] = None,
    processes: Optional[int] = None,
):

    all_files = os.listdir(out_dir)

    cycles_count = set()

    wart_files = []
    for filename in all_files:
        if not is_filename_time_range(filename, time_range):
            continue
        if cycles and not any(map(lambda cycle: cycle in filename, cycles)):
            continue
        cycles_count.add(filename.split(".")[2])
        wart_files.append(filename)

    results = Pool(processes).map(
        partial(extract, time_range),
        [out_dir / wart_file for wart_file in wart_files],
    )

    nodes = set()
    links = set()

    # In the case of cycles specification
    global_min_timestamp = None
    global_max_timestamp = None

    # Global number of traceroutes and probes
    global_n_traceroutes = 0
    global_n_probes = 0

    for (
        nodes_warts,
        links_warts,
        min_timestamp,
        max_timestamp,
        n_traceroutes,
        n_probes,
    ) in results:
        nodes.update(nodes_warts)
        links.update(links_warts)

        if not global_min_timestamp or min_timestamp < global_min_timestamp:
            global_min_timestamp = min_timestamp
        if not global_max_timestamp or max_timestamp > global_max_timestamp:
            global_max_timestamp = max_timestamp

        global_n_traceroutes += n_traceroutes
        global_n_probes += n_probes

    if cycles:
        print(
            "Duration (cycles specified):",
            global_min_timestamp,
            global_max_timestamp,
        )
    print("Number of traceroutes:", global_n_traceroutes)
    print("Number of probes:", global_n_probes)
    print("Number of cycles:", len(cycles_count))

    return nodes, links
