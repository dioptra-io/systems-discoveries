import typer

from platforms_discoveries.ark.download import download_dataset as ark_dataset
from platforms_discoveries.ark.discoveries import get_nodes_links as ark_nodes_links
from platforms_discoveries.ripe.download import download_dataset as ripe_dataset
from platforms_discoveries.ripe.discoveries import get_nodes_links as ripe_nodes_links


from datetime import date
from datetime import datetime
from datetimerange import DateTimeRange
from pathlib import Path


app = typer.Typer()


@app.command()
def ark(
    probing_date: str = typer.Argument(
        ..., help="Probing date in isoformat (e.g., 2021-08-06)"
    ),
    credentials: str = typer.Argument(
        ..., help="Caida credentials (e.g., <email>:<password>)"
    ),
    dataset_dir: Path = typer.Option(
        Path("./data/ark"), help="Directory where to store dataset"
    ),
    timerange_start: str = typer.Option(
        None, help="Time range start in isoformat (e.g., 2021-08-06T11:59:59)"
    ),
    timerange_stop: str = typer.Option(
        None, help="Time range stop in isoformat (e.g., 2021-08-06T16:35:31)"
    ),
):
    """Compute the number of nodes and links from Ark dataset."""
    probing_date = date.fromisoformat(probing_date)
    ark_dir = dataset_dir / probing_date.strftime("%Y%m%d")

    timerange = None
    if timerange_start and timerange_stop:
        timerange = DateTimeRange(
            datetime.fromisoformat(timerange_start),
            datetime.fromisoformat(timerange_stop),
        )

    # Download Ark files
    ark_dataset(probing_date, ark_dir, credentials, timerange=timerange)

    # Compute nodes and links
    nodes, links = ark_nodes_links(ark_dir, timerange=timerange)

    typer.echo(f"Nodes: {len(nodes)}")
    typer.echo(f"Links: {len(links)}")


@app.command()
def ripe(
    probing_date: str = typer.Argument(
        ..., help="Probing date in isoformat (e.g., 2021-08-06)"
    ),
    dataset_dir: Path = typer.Option(
        Path("./data/ripe"), help="Directory where to store dataset"
    ),
    timerange_start: str = typer.Option(
        None, help="Time range start in isoformat (e.g., 2021-08-06T11:59:59)"
    ),
    timerange_stop: str = typer.Option(
        None, help="Time range stop in isoformat (e.g., 2021-08-06T16:35:31)"
    ),
):
    """Compute the number of nodes and links from RIPE dataset."""
    probing_date = date.fromisoformat(probing_date)
    ripe_dir = dataset_dir / probing_date.strftime("%Y%m%d")

    timerange = None
    if timerange_start and timerange_stop:
        timerange = DateTimeRange(
            datetime.fromisoformat(timerange_start),
            datetime.fromisoformat(timerange_stop),
        )

    # Download RIPE files
    ripe_dataset(probing_date, ripe_dir, timerange=timerange)

    # Compute nodes and links
    nodes, links = ripe_nodes_links(ripe_dir, timerange=timerange)

    typer.echo(f"Nodes: {len(nodes)}")
    typer.echo(f"Links: {len(links)}")


if __name__ == "__main__":
    app()
