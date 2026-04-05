# CoordiNode Roadmap

Public roadmap in Now / Next / Later format.

For feature requests and bug reports, use [GitHub Issues](https://github.com/structured-world/coordinode/issues).

---

## Now (v0.3-alpha — current release)

These features are implemented, tested, and available today.

**Query Engine**
- OpenCypher read + write (MATCH, CREATE, MERGE, DELETE, SET, REMOVE, WITH, UNWIND, OPTIONAL MATCH)
- Variable-length path queries (`*1..N`), shortest path
- Aggregation (count, sum, avg, min, max, collect, percentile)
- MVCC transactions with Snapshot Isolation and optimistic conflict detection
- Time-travel queries (AS OF TIMESTAMP, 7-day retention)
- EXPLAIN with cost estimation

**Vector Search**
- HNSW index up to 65,536 dimensions
- SQ8 scalar quantization (4x memory reduction)
- Distance metrics: cosine, L2, dot product, L1
- Vector search on edges (not just nodes)
- Hybrid graph traversal + vector filter in a single query

**Full-Text Search**
- BM25 scoring with fuzzy, phrase, and wildcard queries
- 23+ built-in languages (Snowball stemmers)
- CJK support (Chinese, Japanese, Korean) via feature flags
- Per-field analyzer configuration

**Spatial**
- `point({latitude, longitude})` constructor
- `point.distance()` with Haversine formula
- Spatial predicates in WHERE clauses

**Document Properties**
- Nested DOCUMENT type (arbitrary JSON/MessagePack depth)
- Dot-notation property access (`n.config.network.ssid`)
- Three schema modes: STRICT, VALIDATED, FLEXIBLE

**Indexes**
- B-tree: single-field, compound, unique, partial, sparse
- TTL index with automatic background expiration
- Online index build (zero-downtime)

**Security**
- Searchable symmetric encryption (AES-256-GCM + HMAC-SHA256)
- Equality search on encrypted fields

**Operations**
- Built-in query advisor: EXPLAIN SUGGEST with 5 detectors + N+1 pattern detection
- Prometheus metrics, structured JSON logging, OTLP tracing
- Backup/restore (JSON, Cypher, binary formats)
- Docker image, embedded library mode (`coordinode-embed`)

**Document Operations**
- Path-targeted partial updates (`SET n.config.ssid = "home"` without read-modify-write)
- Array operators (push, pull, addToSet, increment) as merge operands

**API**
- gRPC on port 7080 (native, all services)
- REST/JSON via gRPC-to-REST transcoding (port 7081, via structured-proxy)
- Operational HTTP on port 7084 (/metrics, /health, /ready)
- Parameter binding in gRPC/REST queries

---

## Next

Features in active development or planned for the next few releases.
When a feature is completed, the corresponding documentation will be updated.

**GraphQL API**
- GraphQL with auto-generated schema from graph model (SDL generation implemented, server wiring in progress)
  - *Closes:* COMPATIBILITY.md "GraphQL → Planned"

**WebSocket Subscriptions**
- Live query subscriptions via WebSocket (port 7083)
  - *Closes:* COMPATIBILITY.md "WebSocket → Planned"

**Document Operations**
- Graph-document transformations (promote nested documents to graph nodes and back)

**Schema & Index DDL**
- `CREATE LABEL ... (typed properties)` and `CREATE EDGE_TYPE` Cypher DDL
  - *Closes:* CYPHER_EXTENSIONS.md "Schema (planned DDL)" note
- `CREATE VECTOR INDEX ... OPTIONS {m, ef_construction}` Cypher DDL
  - *Closes:* CYPHER_EXTENSIONS.md "Vector Index (planned DDL)" note

**Replication**
- 3-node Raft clustering with automatic failover (free in CE) — consensus implemented, server binary wiring in progress
  - *Closes:* COMPATIBILITY.md "Free 3-node HA clustering ... server binary wiring in progress"
- Follower reads with staleness tracking
- Causal consistency sessions

**Encrypted Search DDL**
- `CREATE ENCRYPTED INDEX` and `encrypted_match()` Cypher syntax (crypto primitives implemented, query engine wiring planned)
  - *Closes:* CYPHER_EXTENSIONS.md "Encrypted Search (SSE)" note, COMPATIBILITY.md "programmatic API; Cypher DDL planned"

---

## Later

Long-term vision. Timeline depends on community interest and sponsorship.

**Neo4j Compatibility**
- Bolt protocol (v4.3–v5.8) — existing Neo4j drivers connect without code changes
- 130+ OpenCypher functions (string, math, temporal, list, map)
- Neo4j procedures (`db.*`, `dbms.*`)
- LOAD CSV, neo4j-admin import compatibility
- Constraints (node key, existence, property type)

**Horizontal Scaling (Enterprise Edition)**
- Multi-group Raft with hash/range sharding
- CRUSH-like placement with failure domain awareness
- Scatter-gather query engine with predicate push-down
- Cross-shard 2PC transactions with HLC timestamps
- Erasure coding for storage efficiency

**Advanced Features**
- CDC (Change Data Capture) to NATS, Kafka, webhooks
- Materialized graph views with incremental refresh
- Graph analytics (PageRank, community detection, centrality)
- Visual graph explorer (Vue 3 + WebGL)
- Kubernetes operator with rolling upgrades
- Multi-tenancy with mClock QoS per tenant

---

## Contributing

We welcome contributions at any level — from bug reports to feature implementations.

See [CONTRIBUTING.md](../CONTRIBUTING.md) for guidelines.
