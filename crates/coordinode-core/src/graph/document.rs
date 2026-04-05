//! Document property path extraction and traversal.
//!
//! Provides `extract_at_path()` for navigating nested `rmpv::Value` documents
//! using dot-notation path segments. Follows MongoDB-style semantics:
//! - Map keys are traversed by name
//! - Numeric segments index into arrays
//! - Missing keys return Nil
//! - Scalars block further traversal (return Nil)

/// Extract a value at the given path from an rmpv::Value document.
///
/// Path segments navigate through nested maps. Numeric segments can
/// index into arrays (e.g., path `["readings", "0", "value"]` accesses
/// the first element of the "readings" array, then "value" within it).
///
/// Returns `rmpv::Value::Nil` if any intermediate is missing or not traversable.
///
/// # Examples
///
/// ```
/// use coordinode_core::graph::document::extract_at_path;
///
/// let doc = rmpv::Value::Map(vec![
///     (rmpv::Value::String("a".into()), rmpv::Value::Map(vec![
///         (rmpv::Value::String("b".into()), rmpv::Value::Integer(42.into())),
///     ])),
/// ]);
/// let result = extract_at_path(&doc, &["a", "b"]);
/// assert_eq!(result, rmpv::Value::Integer(42.into()));
/// ```
pub fn extract_at_path(value: &rmpv::Value, path: &[&str]) -> rmpv::Value {
    if path.is_empty() {
        return value.clone();
    }

    let segment = path[0];
    let rest = &path[1..];

    match value {
        rmpv::Value::Map(entries) => {
            // Find the key matching this segment
            for (k, v) in entries {
                let key_matches = match k {
                    rmpv::Value::String(s) => s.as_str() == Some(segment),
                    _ => false,
                };
                if key_matches {
                    return extract_at_path(v, rest);
                }
            }
            // Key not found → Nil
            rmpv::Value::Nil
        }
        rmpv::Value::Array(items) => {
            // Numeric segment → array index access
            if let Ok(idx) = segment.parse::<usize>() {
                if let Some(elem) = items.get(idx) {
                    return extract_at_path(elem, rest);
                }
            }
            // Non-numeric segment on array or out-of-bounds → Nil
            rmpv::Value::Nil
        }
        // Scalar or other types cannot be traversed further
        _ => rmpv::Value::Nil,
    }
}

/// Extract a value at the given path from raw MessagePack bytes.
///
/// Optimized projection path: reads map headers and keys at byte level,
/// **skipping** non-matching values without deserializing them. Only the
/// value at the final path segment is fully deserialized.
///
/// Returns `None` if the path doesn't exist or bytes are malformed.
///
/// Performance: for a 10KB document with 1 path, this achieves significant
/// speedup over full deserialization by skipping non-matching entries at
/// byte level using MessagePack size prefixes.
pub fn extract_at_path_bytes(bytes: &[u8], path: &[&str]) -> Option<rmpv::Value> {
    if path.is_empty() {
        let mut cursor = std::io::Cursor::new(bytes);
        return rmpv::decode::read_value(&mut cursor).ok();
    }

    let mut rd = std::io::BufReader::new(std::io::Cursor::new(bytes));
    extract_bytes_recursive(&mut rd, path)
}

/// Byte-level recursive extraction: peek at marker to determine map vs array,
/// then scan entries skipping non-matching values at byte level.
fn extract_bytes_recursive<R: std::io::BufRead>(rd: &mut R, path: &[&str]) -> Option<rmpv::Value> {
    if path.is_empty() {
        return rmpv::decode::read_value(rd).ok();
    }

    let segment = path[0];
    let rest = &path[1..];

    // Peek at the marker to determine if this is a map or array
    let peek = {
        let buf = rd.fill_buf().ok()?;
        if buf.is_empty() {
            return None;
        }
        rmp::Marker::from_u8(buf[0])
    };

    match peek {
        rmp::Marker::FixMap(_) | rmp::Marker::Map16 | rmp::Marker::Map32 => {
            let map_len = rmp::decode::read_map_len(rd).ok()?;
            for _ in 0..map_len {
                let key_len = rmp::decode::read_str_len(rd).ok()? as usize;
                let mut key_buf = vec![0u8; key_len];
                std::io::Read::read_exact(rd, &mut key_buf).ok()?;
                let key_str = std::str::from_utf8(&key_buf).ok()?;

                if key_str == segment {
                    if rest.is_empty() {
                        return rmpv::decode::read_value(rd).ok();
                    }
                    return extract_bytes_recursive(rd, rest);
                }
                skip_msgpack_value(rd)?;
            }
            None
        }
        rmp::Marker::FixArray(_) | rmp::Marker::Array16 | rmp::Marker::Array32 => {
            // Numeric segment → array index
            let idx: usize = segment.parse().ok()?;
            let arr_len = rmp::decode::read_array_len(rd).ok()? as usize;
            if idx >= arr_len {
                // Skip remaining array elements and return None
                for _ in 0..arr_len {
                    skip_msgpack_value(rd)?;
                }
                return None;
            }
            // Skip elements before target index
            for _ in 0..idx {
                skip_msgpack_value(rd)?;
            }
            // Extract element at target index
            if rest.is_empty() {
                return rmpv::decode::read_value(rd).ok();
            }
            extract_bytes_recursive(rd, rest)
        }
        _ => None, // Scalar — can't descend
    }
}

/// Skip one MessagePack value in the stream without allocating.
/// Reads the marker byte, determines the value's structure, and advances
/// the reader past the complete value (including nested maps/arrays).
fn skip_msgpack_value<R: std::io::Read>(rd: &mut R) -> Option<()> {
    let mut marker_buf = [0u8; 1];
    std::io::Read::read_exact(rd, &mut marker_buf).ok()?;
    let marker = rmp::Marker::from_u8(marker_buf[0]);

    match marker {
        rmp::Marker::Null | rmp::Marker::True | rmp::Marker::False => Some(()),
        rmp::Marker::FixPos(_) | rmp::Marker::FixNeg(_) => Some(()),

        rmp::Marker::U8 | rmp::Marker::I8 => skip_n(rd, 1),
        rmp::Marker::U16 | rmp::Marker::I16 => skip_n(rd, 2),
        rmp::Marker::U32 | rmp::Marker::I32 | rmp::Marker::F32 => skip_n(rd, 4),
        rmp::Marker::U64 | rmp::Marker::I64 | rmp::Marker::F64 => skip_n(rd, 8),

        rmp::Marker::FixStr(len) => skip_n(rd, len as usize),
        rmp::Marker::Str8 => {
            let len = read_u8_val(rd)? as usize;
            skip_n(rd, len)
        }
        rmp::Marker::Str16 => {
            let len = read_u16_val(rd)? as usize;
            skip_n(rd, len)
        }
        rmp::Marker::Str32 => {
            let len = read_u32_val(rd)? as usize;
            skip_n(rd, len)
        }

        rmp::Marker::Bin8 => {
            let len = read_u8_val(rd)? as usize;
            skip_n(rd, len)
        }
        rmp::Marker::Bin16 => {
            let len = read_u16_val(rd)? as usize;
            skip_n(rd, len)
        }
        rmp::Marker::Bin32 => {
            let len = read_u32_val(rd)? as usize;
            skip_n(rd, len)
        }

        rmp::Marker::FixArray(len) => skip_n_values(rd, len as usize),
        rmp::Marker::Array16 => {
            let len = read_u16_val(rd)? as usize;
            skip_n_values(rd, len)
        }
        rmp::Marker::Array32 => {
            let len = read_u32_val(rd)? as usize;
            skip_n_values(rd, len)
        }

        rmp::Marker::FixMap(len) => skip_n_values(rd, (len as usize) * 2),
        rmp::Marker::Map16 => {
            let len = read_u16_val(rd)? as usize;
            skip_n_values(rd, len * 2)
        }
        rmp::Marker::Map32 => {
            let len = read_u32_val(rd)? as usize;
            skip_n_values(rd, len * 2)
        }

        rmp::Marker::FixExt1 => skip_n(rd, 2),
        rmp::Marker::FixExt2 => skip_n(rd, 3),
        rmp::Marker::FixExt4 => skip_n(rd, 5),
        rmp::Marker::FixExt8 => skip_n(rd, 9),
        rmp::Marker::FixExt16 => skip_n(rd, 17),
        rmp::Marker::Ext8 => {
            let len = read_u8_val(rd)? as usize;
            skip_n(rd, 1 + len)
        }
        rmp::Marker::Ext16 => {
            let len = read_u16_val(rd)? as usize;
            skip_n(rd, 1 + len)
        }
        rmp::Marker::Ext32 => {
            let len = read_u32_val(rd)? as usize;
            skip_n(rd, 1 + len)
        }

        rmp::Marker::Reserved => None,
    }
}

fn skip_n<R: std::io::Read>(rd: &mut R, n: usize) -> Option<()> {
    // For small skips, use stack buffer. For large, use heap.
    if n <= 256 {
        let mut buf = [0u8; 256];
        std::io::Read::read_exact(rd, &mut buf[..n]).ok()
    } else {
        let mut buf = vec![0u8; n];
        std::io::Read::read_exact(rd, &mut buf).ok()
    }
}

fn skip_n_values<R: std::io::Read>(rd: &mut R, n: usize) -> Option<()> {
    for _ in 0..n {
        skip_msgpack_value(rd)?;
    }
    Some(())
}

fn read_u8_val<R: std::io::Read>(rd: &mut R) -> Option<u8> {
    let mut buf = [0u8; 1];
    std::io::Read::read_exact(rd, &mut buf).ok()?;
    Some(buf[0])
}

fn read_u16_val<R: std::io::Read>(rd: &mut R) -> Option<u16> {
    let mut buf = [0u8; 2];
    std::io::Read::read_exact(rd, &mut buf).ok()?;
    Some(u16::from_be_bytes(buf))
}

fn read_u32_val<R: std::io::Read>(rd: &mut R) -> Option<u32> {
    let mut buf = [0u8; 4];
    std::io::Read::read_exact(rd, &mut buf).ok()?;
    Some(u32::from_be_bytes(buf))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn make_map(entries: Vec<(&str, rmpv::Value)>) -> rmpv::Value {
        rmpv::Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (rmpv::Value::String(k.into()), v))
                .collect(),
        )
    }

    #[test]
    fn single_level_access() {
        let doc = make_map(vec![("name", rmpv::Value::String("Alice".into()))]);
        let result = extract_at_path(&doc, &["name"]);
        assert_eq!(result, rmpv::Value::String("Alice".into()));
    }

    #[test]
    fn three_level_access() {
        let doc = make_map(vec![(
            "a",
            make_map(vec![(
                "b",
                make_map(vec![("c", rmpv::Value::Integer(42.into()))]),
            )]),
        )]);
        let result = extract_at_path(&doc, &["a", "b", "c"]);
        assert_eq!(result, rmpv::Value::Integer(42.into()));
    }

    #[test]
    fn five_level_access() {
        let doc = make_map(vec![(
            "a",
            make_map(vec![(
                "b",
                make_map(vec![(
                    "c",
                    make_map(vec![(
                        "d",
                        make_map(vec![("e", rmpv::Value::String("deep".into()))]),
                    )]),
                )]),
            )]),
        )]);
        let result = extract_at_path(&doc, &["a", "b", "c", "d", "e"]);
        assert_eq!(result, rmpv::Value::String("deep".into()));
    }

    #[test]
    fn missing_key_returns_nil() {
        let doc = make_map(vec![("a", rmpv::Value::Integer(1.into()))]);
        let result = extract_at_path(&doc, &["nonexistent"]);
        assert_eq!(result, rmpv::Value::Nil);
    }

    #[test]
    fn missing_nested_key_returns_nil() {
        let doc = make_map(vec![(
            "a",
            make_map(vec![("b", rmpv::Value::Integer(1.into()))]),
        )]);
        let result = extract_at_path(&doc, &["a", "c"]);
        assert_eq!(result, rmpv::Value::Nil);
    }

    #[test]
    fn path_through_scalar_returns_nil() {
        let doc = make_map(vec![("name", rmpv::Value::String("Alice".into()))]);
        // "name" is a string, can't traverse further
        let result = extract_at_path(&doc, &["name", "x"]);
        assert_eq!(result, rmpv::Value::Nil);
    }

    #[test]
    fn array_numeric_index() {
        let doc = make_map(vec![(
            "readings",
            rmpv::Value::Array(vec![
                make_map(vec![("value", rmpv::Value::F64(23.5))]),
                make_map(vec![("value", rmpv::Value::F64(24.1))]),
            ]),
        )]);
        // readings.0.value → 23.5
        let result = extract_at_path(&doc, &["readings", "0", "value"]);
        assert_eq!(result, rmpv::Value::F64(23.5));

        // readings.1.value → 24.1
        let result = extract_at_path(&doc, &["readings", "1", "value"]);
        assert_eq!(result, rmpv::Value::F64(24.1));
    }

    #[test]
    fn array_out_of_bounds_returns_nil() {
        let doc = make_map(vec![(
            "items",
            rmpv::Value::Array(vec![rmpv::Value::Integer(1.into())]),
        )]);
        let result = extract_at_path(&doc, &["items", "5"]);
        assert_eq!(result, rmpv::Value::Nil);
    }

    #[test]
    fn array_non_numeric_segment_returns_nil() {
        let doc = make_map(vec![(
            "items",
            rmpv::Value::Array(vec![rmpv::Value::Integer(1.into())]),
        )]);
        // "name" is not a valid array index
        let result = extract_at_path(&doc, &["items", "name"]);
        assert_eq!(result, rmpv::Value::Nil);
    }

    #[test]
    fn empty_path_returns_whole_document() {
        let doc = make_map(vec![("a", rmpv::Value::Integer(1.into()))]);
        let result = extract_at_path(&doc, &[]);
        assert_eq!(result, doc);
    }

    #[test]
    fn nil_value_not_traversable() {
        let result = extract_at_path(&rmpv::Value::Nil, &["a"]);
        assert_eq!(result, rmpv::Value::Nil);
    }

    #[test]
    fn heterogeneous_array_index() {
        let doc = make_map(vec![(
            "mixed",
            rmpv::Value::Array(vec![
                rmpv::Value::Integer(1.into()),
                rmpv::Value::String("two".into()),
                rmpv::Value::Boolean(true),
            ]),
        )]);
        assert_eq!(
            extract_at_path(&doc, &["mixed", "0"]),
            rmpv::Value::Integer(1.into())
        );
        assert_eq!(
            extract_at_path(&doc, &["mixed", "1"]),
            rmpv::Value::String("two".into())
        );
        assert_eq!(
            extract_at_path(&doc, &["mixed", "2"]),
            rmpv::Value::Boolean(true)
        );
    }

    // --- extract_at_path_bytes tests ---

    fn to_msgpack(v: &rmpv::Value) -> Vec<u8> {
        let mut buf = Vec::new();
        rmpv::encode::write_value(&mut buf, v).expect("encode");
        buf
    }

    #[test]
    fn bytes_single_level() {
        let doc = make_map(vec![("name", rmpv::Value::String("Alice".into()))]);
        let bytes = to_msgpack(&doc);
        let result = extract_at_path_bytes(&bytes, &["name"]);
        assert_eq!(result, Some(rmpv::Value::String("Alice".into())));
    }

    #[test]
    fn bytes_three_levels() {
        let doc = make_map(vec![(
            "a",
            make_map(vec![(
                "b",
                make_map(vec![("c", rmpv::Value::Integer(42.into()))]),
            )]),
        )]);
        let bytes = to_msgpack(&doc);
        let result = extract_at_path_bytes(&bytes, &["a", "b", "c"]);
        assert_eq!(result, Some(rmpv::Value::Integer(42.into())));
    }

    #[test]
    fn bytes_missing_key() {
        let doc = make_map(vec![("a", rmpv::Value::Integer(1.into()))]);
        let bytes = to_msgpack(&doc);
        assert_eq!(extract_at_path_bytes(&bytes, &["nonexistent"]), None);
    }

    #[test]
    fn bytes_array_index() {
        let doc = make_map(vec![(
            "items",
            rmpv::Value::Array(vec![
                rmpv::Value::String("first".into()),
                rmpv::Value::String("second".into()),
            ]),
        )]);
        let bytes = to_msgpack(&doc);
        assert_eq!(
            extract_at_path_bytes(&bytes, &["items", "1"]),
            Some(rmpv::Value::String("second".into()))
        );
    }

    #[test]
    fn bytes_empty_path() {
        let doc = make_map(vec![("k", rmpv::Value::Integer(1.into()))]);
        let bytes = to_msgpack(&doc);
        let result = extract_at_path_bytes(&bytes, &[]);
        assert_eq!(result, Some(doc));
    }

    #[test]
    fn bytes_skips_non_matching_entries() {
        // Large doc with many keys — only one path extracted
        let mut entries = Vec::new();
        for i in 0..100 {
            entries.push((
                format!("field_{i}"),
                rmpv::Value::String(format!("value_{i}").into()),
            ));
        }
        // Add target at the end
        entries.push(("target".to_string(), rmpv::Value::Integer(999.into())));

        let doc = rmpv::Value::Map(
            entries
                .into_iter()
                .map(|(k, v)| (rmpv::Value::String(k.into()), v))
                .collect(),
        );
        let bytes = to_msgpack(&doc);
        let result = extract_at_path_bytes(&bytes, &["target"]);
        assert_eq!(result, Some(rmpv::Value::Integer(999.into())));
    }

    #[test]
    fn bytes_malformed_returns_none() {
        let result = extract_at_path_bytes(&[0xFF, 0xFF], &["a"]);
        assert!(result.is_none());
    }
}
