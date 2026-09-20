# BT-PSL-Master

Builds portfolio stop-loss (PSL) master files from options backtest runs. It is a companion to the
[PgcBacktest](https://github.com/vikassharma545/PgcBacktest) engine.

## Layout

Three independent pipelines, one per market set:

| Folder | Markets |
|---|---|
| `PSL Master - NSE&BSE` | NSE and BSE index options, intraday |
| `PSL Weekly Master - NSE&BSE` | NSE and BSE index options, weekly |
| `PSL Master - MCX` | MCX commodity options |

Each folder contains the same three numbered steps, run in order:

1. `1. Create Paremeter.py` generates the parameter sets into `parameters/` from `Parameter_MetaData.csv`
   (index, DTE, date range, session times).
2. `codes/` holds the strategy scripts that are run against those parameters.
3. `2. Combine MasterPSL.py` merges the per-run outputs, and `3. CreateMaster.py` builds the final master file.

## Setup

- Python 3.10+ with `pandas`, `dask`, `natsort`.
- Set `pickle_path` in each folder's `config.json` to the directory holding your market-data pickles.

## License

MIT. See [LICENSE](LICENSE).
