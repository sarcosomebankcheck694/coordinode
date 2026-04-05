//! Integration tests: COMPUTED property query-time evaluation (R082).
//!
//! Tests that COMPUTED properties (Decay, TTL, VectorDecay) are evaluated
//! inline during query execution and visible in RETURN and WHERE clauses.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use coordinode_core::graph::types::Value;
use coordinode_core::schema::computed::{ComputedSpec, DecayFormula, TtlScope};
use coordinode_core::schema::definition::{LabelSchema, PropertyDef, PropertyType};
use coordinode_embed::Database;

fn open_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path()).expect("open db");
    (db, dir)
}

/// Helper: create a schema with COMPUTED properties and persist it.
fn setup_memory_schema(db: &mut Database) {
    let mut schema = LabelSchema::new("Memory");
    schema.add_property(PropertyDef::new("content", PropertyType::String).not_null());
    schema.add_property(PropertyDef::new("created_at", PropertyType::Timestamp));
    schema.add_property(PropertyDef::computed(
        "relevance",
        ComputedSpec::Decay {
            formula: DecayFormula::Linear,
            initial: 1.0,
            target: 0.0,
            duration_secs: 604800, // 7 days
            anchor_field: "created_at".into(),
        },
    ));
    schema.add_property(PropertyDef::computed(
        "_ttl",
        ComputedSpec::Ttl {
            duration_secs: 2592000, // 30 days
            anchor_field: "created_at".into(),
            scope: TtlScope::Node,
        },
    ));

    // Persist schema directly to storage.
    let schema_key = coordinode_core::schema::definition::encode_label_schema_key("Memory");
    let bytes = schema.to_msgpack().expect("serialize schema");
    db.engine_shared()
        .put(
            coordinode_storage::engine::partition::Partition::Schema,
            &schema_key,
            &bytes,
        )
        .expect("persist schema");
}

// ── COMPUTED Decay in RETURN ──────────────────────────────────────

/// COMPUTED Decay field appears in RETURN with evaluated value.
#[test]
fn computed_decay_in_return() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    // Create node with timestamp = now (microseconds).
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'test note', created_at: {now_us}}})"
    ))
    .expect("create memory node");

    // Query: RETURN the computed relevance field.
    let rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m.relevance AS rel, m.content AS c")
        .expect("query with computed");

    assert_eq!(rows.len(), 1);
    let rel = rows[0].get("rel").expect("relevance field should exist");
    match rel {
        Value::Float(f) => {
            // Just created → elapsed ≈ 0 → relevance ≈ 1.0
            assert!(
                *f > 0.99,
                "freshly created node should have relevance ≈ 1.0, got {f}"
            );
        }
        other => panic!("expected Float for computed relevance, got {other:?}"),
    }
}

// ── COMPUTED TTL in RETURN ────────────────────────────────────────

/// COMPUTED TTL field returns seconds remaining.
#[test]
fn computed_ttl_returns_seconds_remaining() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'ttl test', created_at: {now_us}}})"
    ))
    .expect("create node");

    let rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m._ttl AS ttl")
        .expect("query ttl");

    assert_eq!(rows.len(), 1);
    match rows[0].get("ttl").expect("ttl should exist") {
        Value::Int(remaining) => {
            // 30 days = 2592000 secs, just created → remaining ≈ 2592000
            assert!(
                *remaining > 2591000,
                "TTL should be ≈ 2592000 secs, got {remaining}"
            );
        }
        other => panic!("expected Int for TTL, got {other:?}"),
    }
}

// ── COMPUTED in WHERE ─────────────────────────────────────────────

/// COMPUTED Decay field usable in WHERE clause.
#[test]
fn computed_decay_in_where_filter() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    // Create two nodes: one fresh (relevance ≈ 1.0) and one "old" (relevance ≈ 0).
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'fresh', created_at: {now_us}}})"
    ))
    .expect("create fresh");

    // 8 days ago → beyond 7-day duration → relevance = 0.0
    let old_us = now_us - 8 * 86400 * 1_000_000;
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'old', created_at: {old_us}}})"
    ))
    .expect("create old");

    // Filter: relevance > 0.5 should return only the fresh node.
    let rows = db
        .execute_cypher("MATCH (m:Memory) WHERE m.relevance > 0.5 RETURN m.content AS c")
        .expect("filter by relevance");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("c"),
        Some(&Value::String("fresh".into())),
        "only the fresh node should pass relevance > 0.5"
    );
}

// ── Missing anchor field → Null ───────────────────────────────────

/// COMPUTED field returns Null when anchor field is missing.
#[test]
fn computed_with_missing_anchor_returns_null() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    // Create node WITHOUT created_at → COMPUTED can't evaluate.
    db.execute_cypher("CREATE (m:Memory {content: 'no timestamp'})")
        .expect("create without anchor");

    let rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m.relevance AS rel")
        .expect("query computed without anchor");

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("rel"),
        Some(&Value::Null),
        "COMPUTED with missing anchor should be Null"
    );
}

// ── COMPUTED in traversal target ──────────────────────────────────

/// COMPUTED properties work on traversal target nodes (build_target_row).
#[test]
fn computed_on_traversal_target() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    db.execute_cypher("CREATE (a:Agent {name: 'alice'})")
        .expect("create agent");
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'memory', created_at: {now_us}}})"
    ))
    .expect("create memory");
    db.execute_cypher(
        "MATCH (a:Agent {name: 'alice'}), (m:Memory {content: 'memory'}) CREATE (a)-[:RECALLS]->(m)"
    )
    .expect("create edge");

    // Traverse and check computed field on target.
    let rows = db
        .execute_cypher(
            "MATCH (a:Agent)-[:RECALLS]->(m:Memory) RETURN m.relevance AS rel, m.content AS c",
        )
        .expect("traverse with computed");

    assert_eq!(rows.len(), 1);
    match rows[0].get("rel") {
        Some(Value::Float(f)) => {
            assert!(
                *f > 0.99,
                "traversal target should have relevance ≈ 1.0, got {f}"
            );
        }
        other => panic!("expected Float for traversal computed, got {other:?}"),
    }
}

// ── COMPUTED TTL Background Reaper (R083) ────────────────────────────

/// Background reaper deletes expired nodes (scope: Node).
#[test]
fn computed_ttl_reaper_deletes_expired_node() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    // Create a node with created_at = 31 days ago (TTL = 30 days → expired).
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let old_us = now_us - 31 * 86400 * 1_000_000;

    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'expired', created_at: {old_us}}})"
    ))
    .expect("create expired node");

    // Create a fresh node (not expired).
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'fresh', created_at: {now_us}}})"
    ))
    .expect("create fresh node");

    // Verify both exist.
    let rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m.content AS c")
        .expect("count before reap");
    assert_eq!(rows.len(), 2, "should have 2 nodes before reap");

    // Run the reaper directly (don't wait for background thread).
    let result =
        coordinode_query::index::ttl_reaper::reap_computed_ttl(&db.engine_shared(), 1, 1000);
    assert_eq!(result.nodes_deleted, 1, "should delete 1 expired node");
    assert_eq!(result.nodes_checked, 2, "should check both nodes");

    // Verify only fresh node remains.
    let rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m.content AS c")
        .expect("count after reap");
    assert_eq!(rows.len(), 1, "should have 1 node after reap");
    assert_eq!(
        rows[0].get("c"),
        Some(&Value::String("fresh".into())),
        "fresh node should survive"
    );
}

/// Reaper handles nodes with edges (DETACH DELETE equivalent).
#[test]
fn computed_ttl_reaper_cleans_edges_on_node_delete() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let old_us = now_us - 31 * 86400 * 1_000_000;

    // Create agent (no TTL) and expired memory with edge.
    db.execute_cypher("CREATE (a:Agent {name: 'alice'})")
        .expect("create agent");
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'old memory', created_at: {old_us}}})"
    ))
    .expect("create expired memory");
    db.execute_cypher(
        "MATCH (a:Agent {name: 'alice'}), (m:Memory {content: 'old memory'}) \
         CREATE (a)-[:RECALLS]->(m)",
    )
    .expect("create edge");

    // Verify edge exists.
    let rows = db
        .execute_cypher("MATCH (a:Agent)-[:RECALLS]->(m:Memory) RETURN m.content AS c")
        .expect("traverse before reap");
    assert_eq!(rows.len(), 1);

    // Run reaper.
    let result =
        coordinode_query::index::ttl_reaper::reap_computed_ttl(&db.engine_shared(), 1, 1000);
    assert_eq!(result.nodes_deleted, 1);

    // Agent should survive, memory should be gone.
    let agent_rows = db
        .execute_cypher("MATCH (a:Agent) RETURN a.name AS n")
        .expect("agents after reap");
    assert_eq!(agent_rows.len(), 1, "agent should survive");

    let memory_rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m.content AS c")
        .expect("memories after reap");
    assert_eq!(memory_rows.len(), 0, "expired memory should be gone");
}

/// RemoveProperty merge operand survives persist + reopen (LSM compaction).
/// Field scope: reaper removes anchor field via merge, close DB, reopen,
/// verify field is still absent.
#[test]
fn computed_ttl_field_removal_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Phase 1: create node, run reaper, verify field removed.
    {
        let mut db = Database::open(dir.path()).expect("open");

        // TTL schema with Field scope, 1-second TTL.
        let mut schema = LabelSchema::new("CacheEntry");
        schema.add_property(PropertyDef::new("data", PropertyType::String));
        schema.add_property(PropertyDef::new("cached_at", PropertyType::Timestamp));
        schema.add_property(PropertyDef::computed(
            "_ttl",
            ComputedSpec::Ttl {
                duration_secs: 1,
                anchor_field: "cached_at".into(),
                scope: TtlScope::Field,
            },
        ));

        let schema_key = coordinode_core::schema::definition::encode_label_schema_key("CacheEntry");
        let bytes = schema.to_msgpack().expect("serialize schema");
        db.engine_shared()
            .put(
                coordinode_storage::engine::partition::Partition::Schema,
                &schema_key,
                &bytes,
            )
            .expect("persist schema");

        // 2 seconds ago → expired with 1s TTL.
        let old_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            - 2_000_000;

        db.execute_cypher(&format!(
            "CREATE (c:CacheEntry {{data: 'stale', cached_at: {old_us}}})"
        ))
        .expect("create");

        // Run reaper → removes cached_at field via RemoveProperty merge.
        let result =
            coordinode_query::index::ttl_reaper::reap_computed_ttl(&db.engine_shared(), 1, 1000);
        assert_eq!(result.fields_removed, 1);

        // Node survives but field is gone.
        let rows = db
            .execute_cypher("MATCH (c:CacheEntry) RETURN c.data AS d")
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("d"), Some(&Value::String("stale".into())));
    }
    // Database dropped → flush + close.

    // Phase 2: reopen and verify merge operand was compacted correctly.
    {
        let mut db = Database::open(dir.path()).expect("reopen");
        let rows = db
            .execute_cypher("MATCH (c:CacheEntry) RETURN c.data AS d")
            .expect("query after reopen");
        assert_eq!(rows.len(), 1, "node should survive reopen");
        assert_eq!(
            rows[0].get("d"),
            Some(&Value::String("stale".into())),
            "data field preserved"
        );
    }
}

/// RemoveProperty merge for Node scope: node deleted, verify gone after reopen.
#[test]
fn computed_ttl_node_deletion_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    {
        let mut db = Database::open(dir.path()).expect("open");
        setup_memory_schema(&mut db);

        let old_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
            - 31 * 86400 * 1_000_000;

        db.execute_cypher(&format!(
            "CREATE (m:Memory {{content: 'old', created_at: {old_us}}})"
        ))
        .expect("create");

        let result =
            coordinode_query::index::ttl_reaper::reap_computed_ttl(&db.engine_shared(), 1, 1000);
        assert_eq!(result.nodes_deleted, 1);
    }

    {
        let mut db = Database::open(dir.path()).expect("reopen");
        let rows = db
            .execute_cypher("MATCH (m:Memory) RETURN m.content AS c")
            .expect("query after reopen");
        assert_eq!(
            rows.len(),
            0,
            "expired node should stay deleted after reopen"
        );
    }
}

/// Pipeline path: reaper submits mutations through ProposalPipeline
/// (same path used by background thread in production).
#[test]
fn computed_ttl_reaper_via_pipeline() {
    let (mut db, _dir) = open_db();
    setup_memory_schema(&mut db);

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let old_us = now_us - 31 * 86400 * 1_000_000;

    // Create expired + fresh nodes.
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'expired', created_at: {old_us}}})"
    ))
    .expect("create expired");
    db.execute_cypher(&format!(
        "CREATE (m:Memory {{content: 'fresh', created_at: {now_us}}})"
    ))
    .expect("create fresh");

    // Create pipeline (same as Database uses internally).
    let pipeline = std::sync::Arc::new(coordinode_raft::proposal::OwnedLocalProposalPipeline::new(
        &db.engine_shared(),
    ));
    let id_gen = coordinode_core::txn::proposal::ProposalIdGenerator::new();
    let interner = coordinode_core::graph::intern::FieldInterner::new();

    let result = coordinode_query::index::ttl_reaper::reap_computed_ttl_via_pipeline(
        &db.engine_shared(),
        1,
        1000,
        &interner,
        pipeline.as_ref(),
        &id_gen,
    );

    assert_eq!(
        result.nodes_deleted, 1,
        "should delete 1 expired node via pipeline"
    );

    let rows = db
        .execute_cypher("MATCH (m:Memory) RETURN m.content AS c")
        .expect("query after pipeline reap");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("c"), Some(&Value::String("fresh".into())),);
}
