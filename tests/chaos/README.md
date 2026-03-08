# Chaos Experiments

Suggested chaos scenarios:

- add latency and packet loss between agents and the aggregator
- restart PostgreSQL while replay log is enabled
- restart one aggregator replica while another keeps serving
- block one RPC provider and verify provider concentration and consistency metrics shift

For network fault injection, pair SentinelMesh with Toxiproxy or a Kubernetes service mesh fault policy.
