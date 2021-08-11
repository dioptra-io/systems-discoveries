# Systems Discoveries

Get the nodes and links discoveries of Ark and RIPE fast and easily.


## Ark dataset

```
$ python main.py ark --help
```

```
Usage: main.py ark [OPTIONS] CREDENTIALS

  Compute the number of nodes and links from Ark dataset.

Arguments:
  CREDENTIALS  Caida credentials (e.g., <email>:<password>)  [required]

Options:
  --time-range-start TEXT  Time range start in isoformat (e.g.,
                           2021-08-06T11:59:59)  [required]

  --time-range-stop TEXT   Time range stop in isoformat (e.g.,
                           2021-08-06T16:35:31)  [required]

  --dataset-dir PATH       Directory where to store dataset  [default:
                           data/ark]

  --processes INTEGER      Number of processes for the multiprocessing pool
  --help                   Show this message and exit.
```

## RIPE dataset

```
$ python main.py ripe --help
```

```
Usage: main.py ripe [OPTIONS]

  Compute the number of nodes and links from RIPE dataset.

Options:
  --time-range-start TEXT  Time range start in isoformat (e.g.,
                           2021-08-06T11:59:59)  [required]

  --time-range-stop TEXT   Time range stop in isoformat (e.g.,
                           2021-08-06T16:35:31)  [required]

  --dataset-dir PATH       Directory where to store dataset  [default:
                           data/ripe]

  --processes INTEGER      Number of processes for the multiprocessing pool
  --help                   Show this message and exit.
```
