# Systems Discoveries

Get the nodes and links discoveries of Ark and RIPE fast and easily.


## Ark dataset

```
$ python main.py ark --help
```

```
Usage: main.py ark [OPTIONS] PROBING_DATE CREDENTIALS

  Compute the number of nodes and links from Ark dataset.

Arguments:
  PROBING_DATE  Probing date in isoformat (e.g., 2021-08-06)  [required]
  CREDENTIALS   Caida credentials (e.g., <email>:<password>)  [required]

Options:
  --dataset-dir PATH      Directory where to store dataset  [default:
                          data/ark]

  --timerange-start TEXT  Time range start in isoformat (e.g.,
                          2021-08-06T11:59:59)

  --timerange-stop TEXT   Time range stop in isoformat (e.g.,
                          2021-08-06T16:35:31)

  --help                  Show this message and exit.
```

## RIPE dataset

```
$ python main.py ripe --help
```

```
Usage: main.py ripe [OPTIONS] PROBING_DATE

  Compute the number of nodes and links from RIPE dataset.

Arguments:
  PROBING_DATE  Probing date in isoformat (e.g., 2021-08-06)  [required]

Options:
  --dataset-dir PATH      Directory where to store dataset  [default:
                          data/ripe]

  --timerange-start TEXT  Time range start in isoformat (e.g.,
                          2021-08-06T11:59:59)

  --timerange-stop TEXT   Time range stop in isoformat (e.g.,
                          2021-08-06T16:35:31)

  --help                  Show this message and exit.
```
