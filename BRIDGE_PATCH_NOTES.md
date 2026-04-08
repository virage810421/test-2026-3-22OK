# Bridge Patch Notes

This package includes direct bridge fixes for:

- `screening.py`
  - SQL -> local cache -> web kline loading
- `fts_local_history_bootstrap.py`
  - writes `data/last_price_snapshot.csv`
  - direct CLI entry added
- `fts_price_gap_bridge.py`
  - adds last snapshot / kline cache / SQL price bridging
  - direct CLI entry added
  - always writes `data/last_price_snapshot.csv`
  - always writes `data/manual_price_snapshot_overrides.csv`
- `master_pipeline.py`
  - runs local bridge preload after watchlist build
  - ensures snapshot/manual override skeleton files exist
  - bridge landing logs added

Recommended entrypoint:

```bash
python master_pipeline.py
```

Optional direct checks:

```bash
python fts_local_history_bootstrap.py
python fts_price_gap_bridge.py
```
