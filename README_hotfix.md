This hotfix resolves the AttributeError in fts_feature_stack_audit.py when the local
fts_feature_catalog.FeatureSpec does not yet contain percentile_backed / event_calendar_precise.

Files included:
- fts_feature_stack_audit.py (compat-safe audit)
- fts_feature_catalog.py (compat-expanded FeatureSpec)
