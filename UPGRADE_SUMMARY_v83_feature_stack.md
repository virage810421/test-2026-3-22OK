# v83 Feature Stack Upgrade Summary

## This patch upgrades
- Feature bucketing for current live features
- `selected_features.pkl` driven live feature selection
- 20 priority new features
- Runtime audit for feature coverage and selected-feature readiness

## Files included
- `fts_feature_catalog.py`
- `fts_feature_service.py`
- `fts_screening_engine.py`
- `fts_feature_stack_audit.py`
- `formal_trading_system_v83_official_main.py`

## Notes
- `ATR14` already existed only implicitly inside ADX math in older code; it is now promoted to a standalone feature.
- `Realized_EV` already existed; `RealizedVol_20` and `RealizedVol_60` are now standalone volatility features.
- Live selection now prefers `models/selected_features.pkl` when present and supports combo features like `A_X_B`.
