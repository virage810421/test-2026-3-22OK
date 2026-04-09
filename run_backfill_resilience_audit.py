from fts_backfill_resilience_audit import BackfillResilienceAudit

if __name__ == '__main__':
    path, payload = BackfillResilienceAudit().build()
    print(path)
    print(payload.get('status'))
