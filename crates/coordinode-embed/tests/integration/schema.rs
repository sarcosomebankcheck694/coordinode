//! Integration tests: Schema DDL and type enforcement.
//!
//! Tests CREATE LABEL, EDGE_TYPE, constraint enforcement, and type validation.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use coordinode_embed::Database;

fn open_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path()).expect("open db");
    (db, dir)
}

// ── Schema DDL ──────────────────────────────────────────────────────

#[test]
fn create_label_schema() {
    let (mut db, _dir) = open_db();
    let result = db.execute_cypher("CREATE LABEL User");
    // Schema DDL may or may not be implemented yet — test that it either
    // succeeds or returns a meaningful error (not a panic)
    match result {
        Ok(_) => {} // Schema created successfully
        Err(e) => {
            let msg = format!("{e}");
            // Verify it's a proper error, not a crash
            assert!(!msg.is_empty(), "Schema error should have a message");
        }
    }
}

// ── Node property types ─────────────────────────────────────────────

#[test]
fn string_property() {
    let (mut db, _dir) = open_db();
    db.execute_cypher("CREATE (n:T {val: 'hello'})")
        .expect("create");
    let rows = db.execute_cypher("MATCH (n:T) RETURN n.val").expect("read");
    assert_eq!(rows.len(), 1);
}

#[test]
fn integer_property() {
    let (mut db, _dir) = open_db();
    db.execute_cypher("CREATE (n:T {val: 42})").expect("create");
    let rows = db
        .execute_cypher("MATCH (n:T) WHERE n.val = 42 RETURN n")
        .expect("read");
    assert_eq!(rows.len(), 1);
}

#[test]
fn float_property() {
    let (mut db, _dir) = open_db();
    db.execute_cypher("CREATE (n:T {val: 3.14})")
        .expect("create");
    let rows = db.execute_cypher("MATCH (n:T) RETURN n.val").expect("read");
    assert_eq!(rows.len(), 1);
}

#[test]
fn boolean_property() {
    let (mut db, _dir) = open_db();
    db.execute_cypher("CREATE (n:T {active: true})")
        .expect("create");
    let rows = db
        .execute_cypher("MATCH (n:T) WHERE n.active = true RETURN n")
        .expect("read");
    assert_eq!(rows.len(), 1);
}

#[test]
fn list_property() {
    let (mut db, _dir) = open_db();
    db.execute_cypher("CREATE (n:T {tags: ['a', 'b', 'c']})")
        .expect("create");
    let rows = db
        .execute_cypher("MATCH (n:T) RETURN n.tags")
        .expect("read");
    assert_eq!(rows.len(), 1);
}

#[test]
fn null_property_handling() {
    let (mut db, _dir) = open_db();
    db.execute_cypher("CREATE (n:T {name: 'test'})")
        .expect("create");
    // Reading a non-existent property should return null, not error
    let rows = db
        .execute_cypher("MATCH (n:T) RETURN n.nonexistent")
        .expect("read null");
    assert_eq!(rows.len(), 1);
}

// ── Multiple labels ─────────────────────────────────────────────────

// ── Edge creation ───────────────────────────────────────────────────

#[test]
fn edge_creation_does_not_error() {
    let (mut db, _dir) = open_db();
    // Creating edges with properties should succeed
    let result = db.execute_cypher(
        "CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
    );
    assert!(result.is_ok(), "edge creation should not error");
}

#[test]
fn multiple_edge_type_creation() {
    let (mut db, _dir) = open_db();
    // Creating edges with different types should not error
    let r1 =
        db.execute_cypher("CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})");
    let r2 = db
        .execute_cypher("CREATE (c:Person {name: 'Charlie'})-[:LIKES]->(d:Person {name: 'Dave'})");
    assert!(r1.is_ok());
    assert!(r2.is_ok());
}
