from fts_training_stress_audit import TrainingStressAudit

if __name__ == '__main__':
    path, payload = TrainingStressAudit().build()
    print(path)
    print(payload.get('status'))
