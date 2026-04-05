# CoordiNode Cypher Extensions

CoordiNode extends the OpenCypher query language with native support for vector search, full-text search, spatial queries, time-travel reads, and encrypted search. These extensions compose naturally with standard Cypher — any combination works in a single query.

## Vector Search

### Schema (planned DDL)

> **Note:** Schema DDL (`CREATE LABEL`, `CREATE EDGE_TYPE`) is planned. Currently, schemas are created via the programmatic API (`LabelSchema`). Nodes can be created directly without pre-declaring schemas.

```cypher
-- Planned DDL syntax:
CREATE LABEL Product (
  name STRING NOT NULL,
  description STRING,
  embedding VECTOR(384, cosine)     -- 384 dimensions, cosine distance
)

-- Supported metrics: cosine, l2, dot, l1
CREATE LABEL Image (
  pixels VECTOR(2048, l2)
)

-- Vectors on edges (unique to CoordiNode)
CREATE EDGE_TYPE SIMILAR (
  score FLOAT,
  joint_embedding VECTOR(768, cosine)
)
```

### Vector Index (planned DDL)

> **Note:** Vector indexes are currently created via the programmatic API (`Database::create_vector_index()`). Cypher DDL syntax is planned.

```cypher
-- Planned DDL syntax:
CREATE VECTOR INDEX product_embedding ON Product(embedding)
  OPTIONS {m: 16, ef_construction: 200}

-- Edge vector index
CREATE VECTOR INDEX ON EDGE SIMILAR(joint_embedding)

-- Flat brute-force index (exact, for <100K vectors)
CREATE VECTOR INDEX product_flat ON Product(embedding) USING FLAT
```

### Query Functions

```cypher
-- Distance filter: find similar products
MATCH (p:Product)
WHERE vector_distance(p.embedding, $query_vector) < 0.3
RETURN p.name, vector_distance(p.embedding, $query_vector) AS distance
ORDER BY distance LIMIT 10

-- Similarity (1 - distance, for cosine)
MATCH (p:Product)
WHERE vector_similarity(p.embedding, $query_vector) > 0.7
RETURN p.name

-- KNN: top-K nearest neighbors
MATCH (p:Product)
RETURN p.name, vector_distance(p.embedding, $query_vector) AS dist
ORDER BY dist LIMIT 10
```

### Hybrid Graph + Vector

```cypher
-- Traverse graph, then filter by vector similarity
MATCH (user:User {id: $uid})-[:PURCHASED]->(bought:Product)
MATCH (similar:Product)
WHERE similar <> bought
  AND vector_distance(similar.embedding, bought.embedding) < 0.2
RETURN DISTINCT similar.name, min(vector_distance(similar.embedding, bought.embedding)) AS score
ORDER BY score LIMIT 10
```

## Full-Text Search

### Index

```cypher
-- Full-text index with language-specific analyzer
CREATE TEXT INDEX doc_body ON Document(body)
  OPTIONS {analyzer: "english"}

-- Multi-language with auto-detection
CREATE TEXT INDEX doc_body_multi ON Document(body)
  OPTIONS {analyzer: "auto", languages: ["en", "de", "fr", "uk"]}
```

### Query Functions

```cypher
-- BM25 text search
MATCH (doc:Document)
WHERE text_match(doc.body, "distributed consensus algorithm")
RETURN doc.title, text_score(doc.body, "distributed consensus algorithm") AS relevance
ORDER BY relevance DESC LIMIT 10

-- Fuzzy search (Levenshtein distance 1-3)
WHERE text_match(doc.body, "konsensus~2")

-- Phrase search
WHERE text_match(doc.body, "\"raft consensus\"")

-- Boolean operators
WHERE text_match(doc.body, "raft AND (consensus OR paxos) NOT zookeeper")

-- Wildcard
WHERE text_match(doc.body, "distribut*")
```

### Supported Languages (23+)

Arabic, Armenian, Basque, Catalan, Danish, Dutch, English, Finnish, French, German, Greek, Hindi, Hungarian, Indonesian, Irish, Italian, Lithuanian, Nepali, Norwegian, Portuguese, Romanian, Russian, Serbian, Spanish, Swedish, Tamil, Turkish, Ukrainian, Yiddish.

**CJK (feature flags):** Chinese (jieba-rs), Japanese (lindera), Korean (lindera).

### Hybrid: Graph + Vector + Full-Text

```cypher
-- The CoordiNode superpower: all three in one query
MATCH (topic:Concept)-[:RELATED_TO*1..2]->(related)
MATCH (related)<-[:ABOUT]-(doc:Document)
WHERE vector_distance(doc.embedding, $query_vec) < 0.4
  AND text_match(doc.body, "attention mechanism")
RETURN doc.title,
       vector_distance(doc.embedding, $query_vec) AS semantic_score,
       text_score(doc.body, "attention mechanism") AS text_score
ORDER BY semantic_score LIMIT 10
```

## Spatial Queries

```cypher
-- Point distance (meters)
MATCH (r:Restaurant)
WHERE point.distance(r.location, point({latitude: 40.7128, longitude: -74.0060})) < 2000
RETURN r.name, point.distance(r.location, point({latitude: 40.7128, longitude: -74.0060})) AS dist
ORDER BY dist

-- Spatial + Graph: nearby places from your social network
MATCH (me:User {id: $uid})-[:FOLLOWS]->(friend)-[rev:REVIEWED]->(place:Restaurant)
WHERE point.distance(place.location, $my_location) < 5000
  AND rev.rating >= 4
RETURN place.name, avg(rev.rating) AS score, count(friend) AS endorsements
ORDER BY score DESC LIMIT 10
```

## Time-Travel Queries

```cypher
-- Read data as it was at a specific timestamp (7-day retention)
MATCH (u:User {id: 42})
RETURN u.name, u.email
AS OF TIMESTAMP '2026-03-15T10:00:00Z'

-- Compare current state with historical state
MATCH (product:Product {sku: "ABC-123"})
RETURN product.price AS current_price
-- (run separate AS OF query for historical price)
```

## Encrypted Search (SSE)

> **Note:** SSE crypto primitives and persistent index are implemented. Cypher DDL (`CREATE ENCRYPTED INDEX`) and query function (`encrypted_match()`) are planned. Currently available via programmatic API (`encrypt_field()`, `EncryptedIndex`).

```cypher
-- Planned DDL syntax:
CREATE ENCRYPTED INDEX patient_ssn ON Patient(ssn)

-- Planned query syntax:
MATCH (p:Patient)
WHERE encrypted_match(p.ssn, $encrypted_token)
RETURN p.id, p.name
```

## Aggregation Functions

Standard: `count()`, `sum()`, `avg()`, `min()`, `max()`, `collect()`, `percentileCont()`, `percentileDisc()`

```cypher
-- Aggregation with graph traversal
MATCH (dept:Department)<-[:WORKS_IN]-(emp:Employee)
RETURN dept.name, count(emp) AS headcount, avg(emp.salary) AS avg_salary
ORDER BY headcount DESC
```

## Query Advisor

```cypher
-- Get optimization suggestions for any query
EXPLAIN SUGGEST
MATCH (u:User) WHERE u.email = $email RETURN u

-- Built-in detectors:
-- MissingIndex:          suggest CREATE INDEX for filtered properties
-- UnboundedTraversal:    warn about *.. without depth limit
-- CartesianProduct:      detect disconnected MATCH patterns
-- KnnWithoutIndex:       suggest vector index for ORDER BY distance + LIMIT
-- VectorWithoutPreFilter: suggest graph narrowing before vector scan
```
