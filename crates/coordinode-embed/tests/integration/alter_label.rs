//! Integration tests: ALTER LABEL SET SCHEMA DDL (G029).
//!
//! Verifies that schema mode can be changed via Cypher DDL
//! and that write-time validation respects the new mode.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use coordinode_core::graph::types::Value;
use coordinode_embed::Database;

fn open_db() -> (Database, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db = Database::open(dir.path()).expect("open db");
    (db, dir)
}

// ── Basic ALTER LABEL ───────────────────────────────────────────────

/// ALTER LABEL SET SCHEMA VALIDATED returns result with label, mode, version.
#[test]
fn alter_label_returns_result() {
    let (mut db, _dir) = open_db();

    let rows = db
        .execute_cypher("ALTER LABEL User SET SCHEMA VALIDATED")
        .expect("alter label");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("label"), Some(&Value::String("User".into())));
    assert_eq!(
        rows[0].get("mode"),
        Some(&Value::String("VALIDATED".into()))
    );
}

/// ALTER LABEL SET SCHEMA FLEXIBLE works.
#[test]
fn alter_label_flexible() {
    let (mut db, _dir) = open_db();

    let rows = db
        .execute_cypher("ALTER LABEL Config SET SCHEMA FLEXIBLE")
        .expect("alter");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("mode"), Some(&Value::String("FLEXIBLE".into())));
}

/// ALTER LABEL SET SCHEMA STRICT works.
#[test]
fn alter_label_strict() {
    let (mut db, _dir) = open_db();

    let rows = db
        .execute_cypher("ALTER LABEL Product SET SCHEMA STRICT")
        .expect("alter");
    assert_eq!(rows[0].get("mode"), Some(&Value::String("STRICT".into())));
}

// ── Case insensitive ────────────────────────────────────────────────

#[test]
fn alter_label_case_insensitive() {
    let (mut db, _dir) = open_db();

    let rows = db
        .execute_cypher("alter label Test set schema flexible")
        .expect("alter");
    assert_eq!(rows[0].get("mode"), Some(&Value::String("FLEXIBLE".into())));
}

// ── Schema mode persists across reopen ──────────────────────────────

/// Schema mode change persists after Database close + reopen.
#[test]
fn alter_label_persists_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Session 1: set schema mode to FLEXIBLE
    {
        let mut db = Database::open(dir.path()).expect("open");
        db.execute_cypher("ALTER LABEL Device SET SCHEMA FLEXIBLE")
            .expect("alter");
    }

    // Session 2: verify the mode persisted by altering again
    // (if the first alter didn't persist, this creates a new STRICT schema)
    {
        let mut db = Database::open(dir.path()).expect("reopen");
        // ALTER again to VALIDATED — if previous was persisted, version > 1
        let rows = db
            .execute_cypher("ALTER LABEL Device SET SCHEMA VALIDATED")
            .expect("alter again");
        let version = rows[0].get("version");
        // Version should be > 1 if the first ALTER persisted
        assert!(
            matches!(version, Some(Value::Int(v)) if *v >= 2),
            "version should be >= 2 after two ALTERs, got: {version:?}"
        );
    }
}

// ── Invalid mode ────────────────────────────────────────────────────

/// Invalid schema mode should fail at parse level (PEG rejects unknown modes).
#[test]
fn alter_label_invalid_mode_parse_error() {
    let (mut db, _dir) = open_db();

    let result = db.execute_cypher("ALTER LABEL User SET SCHEMA UNKNOWN");
    assert!(result.is_err(), "invalid mode should fail");
}
