import bz2
import gzip
import os
import warts

from datetime import datetime
from datetimerange import DateTimeRange
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from typing import Optional


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


def extract(wart_file: Path, timerange: Optional[DateTimeRange] = None):
    fd = warts_open(str(wart_file))

    nodes = set()
    links = set()
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

        if timerange and datetime.fromtimestamp(record.start_time) not in timerange:
            continue

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
    return nodes, links


def get_nodes_links(
    out_dir: Path, timerange: Optional[DateTimeRange] = None, processes: int = None
):
    wart_files = os.listdir(out_dir)
    results = Pool(processes).map(
        partial(extract, timerange=timerange),
        [out_dir / wart_file for wart_file in wart_files],
    )

    nodes = set()
    links = set()
    for nodes_warts, links_warts in results:
        nodes.update(nodes_warts)
        links.update(links_warts)

    return nodes, links
