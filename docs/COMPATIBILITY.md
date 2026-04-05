# Neo4j Compatibility Matrix

CoordiNode is OpenCypher-compatible and supports common Neo4j workloads. This document tracks what works today and what's planned. Full Neo4j ecosystem parity (Bolt, APOC, GDS) is a long-term goal, not a current state.

## Query Language (OpenCypher)

| Feature | Status | Notes |
|---------|--------|-------|
| MATCH / OPTIONAL MATCH | Supported | Full pattern matching |
| WHERE (comparisons, boolean, IS NULL) | Supported | All standard operators |
| RETURN / ORDER BY / LIMIT / SKIP | Supported | With expressions and aliases |
| CREATE / MERGE / DELETE / DETACH DELETE | Supported | Full write operations |
| SET / REMOVE | Supported | Properties and labels |
| WITH / UNWIND | Supported | Pipeline and list expansion |
| Variable-length paths `*1..N` | Supported | With configurable depth bounds |
| Shortest path | Supported | BFS-based |
| CASE WHEN | Supported | Simple and searched forms |
| Aggregations | Supported | count, sum, avg, min, max, collect, percentile |
| UPSERT (atomic) | **Extension** | Not in standard Neo4j Cypher |
| AS OF TIMESTAMP | **Extension** | Time-travel reads (7-day retention) |
| vector_distance() | **Extension** | Native vector search |
| text_match() / text_score() | **Extension** | Native full-text search |
| EXPLAIN / EXPLAIN SUGGEST | **Extension** | Query advisor with suggestions |
| CALL procedures | Planned | v1.2 milestone |
| LOAD CSV | Planned | v1.2 milestone |
| FOREACH | Planned | v1.0 milestone |

## Data Types

| Type | Status | Notes |
|------|--------|-------|
| String, Integer, Float, Boolean | Supported | Standard types |
| List, Map | Supported | Homogeneous lists, string-keyed maps |
| Point (spatial) | Supported | WGS84 lat/lon |
| DateTime / Timestamp | Supported | Microsecond precision |
| Vector | **Extension** | Up to 65536 dimensions |
| Blob | **Extension** | Content-addressed binary storage |
| NULL | Supported | Three-valued logic |

## Indexes

| Type | Status | Notes |
|------|--------|-------|
| B-tree (single property) | Supported | Standard performance index |
| Composite (multi-property) | Supported | Compound B-tree |
| Unique constraint | Supported | With commit-time enforcement |
| Text index (full-text) | Supported | BM25, fuzzy, phrase, 23+ languages |
| Vector index (HNSW) | **Extension** | Approximate nearest neighbor |
| Vector index (Flat) | **Extension** | Exact NN for small datasets |
| Partial index | **Extension** | Index with filter predicate |
| TTL index | **Extension** | Automatic expiration |
| Point index | Planned | Spatial R-tree |
| Lookup index | Planned | Neo4j-specific |

## Protocols

| Protocol | Status | Notes |
|----------|--------|-------|
| Bolt (Neo4j wire protocol) | Planned (v1.2) | Neo4j drivers will connect without changes |
| gRPC | Supported | Native high-performance API |
| HTTP/REST | Supported | JSON API with gRPC transcoding |
| GraphQL | Planned | Auto-generated schema (SDL generation implemented, server wiring in progress) |
| WebSocket | Planned | Subscriptions and live queries |

## Drivers

| Driver | Status | Notes |
|--------|--------|-------|
| Official gRPC client (Rust) | Supported | Generated from proto |
| Any gRPC client | Supported | Proto definitions available |
| Neo4j Python driver | Planned (v1.2) | Via Bolt protocol |
| Neo4j JavaScript driver | Planned (v1.2) | Via Bolt protocol |
| Neo4j Java driver | Planned (v1.2) | Via Bolt protocol |
| Neo4j Go driver | Planned (v1.2) | Via Bolt protocol |

## What's Different from Neo4j

### CoordiNode has, Neo4j doesn't:
- Native vector search (HNSW) on nodes AND edges
- Encrypted search (SSE) — query encrypted fields without decryption (programmatic API; Cypher DDL planned)
- Time-travel queries (AS OF TIMESTAMP)
- Partial indexes with filter predicates
- TTL indexes with automatic cascade delete
- Built-in query advisor (EXPLAIN SUGGEST)
- Free 3-node HA clustering (Raft consensus implemented; server binary wiring in progress — Neo4j charges per-node for clustering)
- UPSERT MATCH as atomic operation

### Neo4j has, CoordiNode doesn't (yet):
- Bolt protocol (planned v1.2)
- APOC procedures library (partial support planned)
- LOAD CSV (planned v1.2)
- GDS (Graph Data Science) library
- Neo4j Browser / Bloom visualization
- Cypher subqueries (CALL {} syntax)
