# Haptic 🤏

Scrapes Prometheus-compatible endpoints and chooches your metrics to Clickhouse.

Features:
- Service discovery (kubernetes!)
- Automagical schema management
- Clickhouse bulk insertion

### Run
```haptic --ch-uri tcp://clichouse.default.svc.cluster.local/database```

### RBAC Requirements

Cluster:
```yaml
rules:
- apiGroups: ['']
  resources: ['pods', 'nodes']
  verbs: ['list', 'get']
  ```