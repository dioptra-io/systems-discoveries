import typer

from platforms_discoveries.ark.download import download_dataset as ark_dataset
from platforms_discoveries.ark.discoveries import get_nodes_links as ark_nodes_links
from platforms_discoveries.ripe.download import download_dataset as ripe_dataset
from platforms_discoveries.ripe.discoveries import get_nodes_links as ripe_nodes_links

from datetime import datetime
from datetimerange import DateTimeRange
from pathlib import Path
from typing import List


app = typer.Typer()


@app.command()
def ark(
    credentials: str = typer.Argument(
        ..., help="Caida credentials (e.g., <email>:<password>)"
    ),
    time_range_start: str = typer.Option(
        ..., help="Time range start in isoformat (e.g., 2021-08-06T11:59:59)"
    ),
    time_range_stop: str = typer.Option(
        ..., help="Time range end in isoformat (e.g., 2021-08-06T16:35:31)"
    ),
    cycle: List[str] = typer.Option(
        None, help="Filter to a specific cycle in the specified time range"
    ),
    dataset_dir: Path = typer.Option(
        Path("./data/ark"), help="Directory where to store dataset"
    ),
    processes: int = typer.Option(
        None, help="Number of processes for the multiprocessing pool"
    ),
):
    """Compute the number of nodes and links from Ark dataset."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    time_range = DateTimeRange(
        datetime.fromisoformat(time_range_start),
        datetime.fromisoformat(time_range_stop),
    )

    # Download Ark files
    ark_dataset(dataset_dir, credentials, time_range, cycle, processes)

    # Compute nodes and links
    nodes, links = ark_nodes_links(dataset_dir, time_range, cycle, processes)

    typer.echo(f"Nodes: {len(nodes)}")
    typer.echo(f"Links: {len(links)}")


@app.command()
def ripe(
    time_range_start: str = typer.Option(
        ..., help="Time range start in isoformat (e.g., 2021-08-06T11:59:59)"
    ),
    time_range_stop: str = typer.Option(
        ..., help="Time range end in isoformat (e.g., 2021-08-06T16:35:31)"
    ),
    dataset_dir: Path = typer.Option(
        Path("./data/ripe"), help="Directory where to store dataset"
    ),
    processes: int = typer.Option(
        None, help="Number of processes for the multiprocessing pool"
    ),
):
    """Compute the number of nodes and links from RIPE dataset."""
    dataset_dir.mkdir(parents=True, exist_ok=True)
    time_range = DateTimeRange(
        datetime.fromisoformat(time_range_start),
        datetime.fromisoformat(time_range_stop),
    )

    # Download RIPE files
    ripe_dataset(dataset_dir, time_range, processes)

    # Compute nodes and links
    nodes, links = ripe_nodes_links(dataset_dir, time_range, processes)

    typer.echo(f"Nodes: {len(nodes)}")
    typer.echo(f"Links: {len(links)}")


if __name__ == "__main__":
    app()
