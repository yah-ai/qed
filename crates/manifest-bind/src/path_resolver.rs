//! Format-aware path resolver — v1 covers TOML via `toml_edit`.
//!
//! Grammar (a subset of jq-ish + TOML-dotted, enough for the cases W209
//! enumerates):
//!
//! - Dotted segments: `asset.derive.fetch.blake3`
//! - Array element by key=value: `asset[filename='whisper.tar.gz'].blake3`
//! - Array element by numeric index: `asset[0].blake3`
//! - Top-level scalar: `image`
//!
//! Quoted string values use single quotes; double-quoted are also accepted.

use toml_edit::{DocumentMut, Item, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
enum Seg {
    Key(String),
    IndexByKey { key: String, value: String },
    IndexByPos(usize),
}

fn parse_path(path: &str) -> Result<Vec<Seg>, String> {
    let mut out = Vec::new();
    let mut chars = path.chars().peekable();
    let mut current = String::new();
    let flush_key = |buf: &mut String, out: &mut Vec<Seg>| {
        if !buf.is_empty() {
            out.push(Seg::Key(std::mem::take(buf)));
        }
    };
    while let Some(c) = chars.next() {
        match c {
            '.' => flush_key(&mut current, &mut out),
            '[' => {
                flush_key(&mut current, &mut out);
                let mut inner = String::new();
                let mut depth = 1;
                for nc in chars.by_ref() {
                    if nc == '[' {
                        depth += 1;
                        inner.push(nc);
                    } else if nc == ']' {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                        inner.push(nc);
                    } else {
                        inner.push(nc);
                    }
                }
                if depth != 0 {
                    return Err(format!("unclosed '[' in path {path:?}"));
                }
                let inner = inner.trim();
                if let Ok(idx) = inner.parse::<usize>() {
                    out.push(Seg::IndexByPos(idx));
                } else if let Some(eq) = inner.find('=') {
                    let key = inner[..eq].trim().to_owned();
                    let val_raw = inner[eq + 1..].trim();
                    let val = strip_quotes(val_raw)
                        .ok_or_else(|| format!("expected quoted value in [{inner}]"))?;
                    out.push(Seg::IndexByKey { key, value: val });
                } else {
                    return Err(format!("malformed index segment [{inner}]"));
                }
            }
            _ => current.push(c),
        }
    }
    flush_key(&mut current, &mut out);
    if out.is_empty() {
        return Err(format!("empty path {path:?}"));
    }
    Ok(out)
}

fn strip_quotes(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'\'' && last == b'\'') || (first == b'"' && last == b'"') {
            return Some(s[1..s.len() - 1].to_owned());
        }
    }
    None
}

/// Read the current scalar at `path`. Returns Ok(None) when the slot is
/// absent (parent containers exist but the leaf isn't there yet) — the
/// applier treats this as "first write" rather than an error. Returns Err
/// only when path traversal hits a shape-incompatible segment.
pub fn toml_get(doc: &DocumentMut, path: &str) -> Result<Option<String>, String> {
    let segs = parse_path(path)?;
    let item: &Item = doc.as_item();
    descend_get(item, &segs)
}

fn descend_get(mut item: &Item, segs: &[Seg]) -> Result<Option<String>, String> {
    for seg in segs {
        match seg {
            Seg::Key(k) => match item {
                Item::Table(t) => match t.get(k) {
                    Some(next) => item = next,
                    None => return Ok(None),
                },
                Item::Value(Value::InlineTable(it)) => match it.get(k) {
                    Some(_) => {
                        return Err(format!(
                            "inline-table descent on key {k:?} not supported in v1"
                        ))
                    }
                    None => return Ok(None),
                },
                _ => {
                    return Err(format!(
                        "expected table at key {k:?}, found {}",
                        item_kind(item)
                    ))
                }
            },
            Seg::IndexByPos(idx) => match item {
                Item::ArrayOfTables(aot) => match aot.get(*idx) {
                    Some(t) => {
                        // Build a fake Item::Table referencing this slot.
                        // toml_edit doesn't give us a direct &Item for the
                        // table, but we can keep descending segs against
                        // the &Table directly.
                        return descend_get_table(t, &remaining(segs, seg));
                    }
                    None => return Ok(None),
                },
                _ => {
                    return Err(format!(
                        "expected array-of-tables for index {idx}, found {}",
                        item_kind(item)
                    ))
                }
            },
            Seg::IndexByKey { key, value } => match item {
                Item::ArrayOfTables(aot) => {
                    let found = aot.iter().find(|t| {
                        t.get(key)
                            .and_then(|i| i.as_value())
                            .and_then(|v| v.as_str())
                            == Some(value.as_str())
                    });
                    match found {
                        Some(t) => return descend_get_table(t, &remaining(segs, seg)),
                        None => return Ok(None),
                    }
                }
                _ => {
                    return Err(format!(
                        "expected array-of-tables for [{key}={value:?}], found {}",
                        item_kind(item)
                    ))
                }
            },
        }
    }
    Ok(item_as_string(item))
}

fn descend_get_table(t: &toml_edit::Table, segs: &[Seg]) -> Result<Option<String>, String> {
    let mut current: &Item = &Item::None;
    let mut start = true;
    for (i, seg) in segs.iter().enumerate() {
        match seg {
            Seg::Key(k) => {
                let next = if start {
                    start = false;
                    t.get(k)
                } else {
                    match current {
                        Item::Table(t2) => t2.get(k),
                        _ => return Err(format!("expected table descending at {k:?}")),
                    }
                };
                match next {
                    Some(n) => current = n,
                    None => return Ok(None),
                }
            }
            _ => {
                let _ = i;
                return Err("nested array indexing not supported in v1".into());
            }
        }
    }
    Ok(item_as_string(current))
}

fn remaining(all: &[Seg], from: &Seg) -> Vec<Seg> {
    let pos = all.iter().position(|s| s == from).unwrap_or(all.len());
    all[pos + 1..].to_vec()
}

fn item_kind(item: &Item) -> &'static str {
    match item {
        Item::None => "none",
        Item::Value(_) => "value",
        Item::Table(_) => "table",
        Item::ArrayOfTables(_) => "array-of-tables",
    }
}

fn item_as_string(item: &Item) -> Option<String> {
    match item {
        Item::Value(Value::String(s)) => Some(s.value().to_owned()),
        Item::Value(Value::Integer(i)) => Some(i.value().to_string()),
        Item::Value(Value::Float(f)) => Some(f.value().to_string()),
        Item::Value(Value::Boolean(b)) => Some(b.value().to_string()),
        Item::Value(Value::Datetime(d)) => Some(d.value().to_string()),
        _ => None,
    }
}

/// Set the scalar at `path` to `new_value` (as a string). Parents must
/// exist; the resolver does not auto-create intermediate tables (W209: a
/// bind that can't resolve its target is a hard error at apply time).
pub fn toml_set(doc: &mut DocumentMut, path: &str, new_value: &str) -> Result<(), String> {
    let segs = parse_path(path)?;
    descend_set(doc.as_item_mut(), &segs, new_value)
}

fn descend_set(item: &mut Item, segs: &[Seg], new_value: &str) -> Result<(), String> {
    if segs.is_empty() {
        return Err("empty path on set".into());
    }
    let (head, tail) = segs.split_first().unwrap();
    let is_last = tail.is_empty();
    match head {
        Seg::Key(k) => {
            let table = match item {
                Item::Table(t) => t,
                _ => return Err(format!("expected table at key {k:?}")),
            };
            if is_last {
                if !table.contains_key(k) {
                    return Err(format!("leaf key {k:?} absent — refusing to create"));
                }
                table[k] = toml_edit::value(new_value);
                Ok(())
            } else {
                let next = table
                    .get_mut(k)
                    .ok_or_else(|| format!("key {k:?} missing on traversal"))?;
                descend_set(next, tail, new_value)
            }
        }
        Seg::IndexByPos(idx) => {
            let aot = match item {
                Item::ArrayOfTables(a) => a,
                _ => return Err(format!("expected array-of-tables at index {idx}")),
            };
            let table = aot
                .get_mut(*idx)
                .ok_or_else(|| format!("array index {idx} out of bounds"))?;
            if is_last {
                Err("cannot replace a whole table — point at a leaf".into())
            } else {
                descend_set_table(table, tail, new_value)
            }
        }
        Seg::IndexByKey { key, value } => {
            let aot = match item {
                Item::ArrayOfTables(a) => a,
                _ => {
                    return Err(format!(
                        "expected array-of-tables for [{key}={value:?}]"
                    ))
                }
            };
            let pos = aot
                .iter()
                .position(|t| {
                    t.get(key)
                        .and_then(|i| i.as_value())
                        .and_then(|v| v.as_str())
                        == Some(value.as_str())
                })
                .ok_or_else(|| format!("no array element with {key}={value:?}"))?;
            let table = aot.get_mut(pos).unwrap();
            if is_last {
                Err("cannot replace a whole table — point at a leaf".into())
            } else {
                descend_set_table(table, tail, new_value)
            }
        }
    }
}

fn descend_set_table(
    table: &mut toml_edit::Table,
    segs: &[Seg],
    new_value: &str,
) -> Result<(), String> {
    let (head, tail) = segs.split_first().ok_or("empty descend")?;
    let is_last = tail.is_empty();
    match head {
        Seg::Key(k) => {
            if is_last {
                if !table.contains_key(k) {
                    return Err(format!("leaf key {k:?} absent in nested table"));
                }
                table[k] = toml_edit::value(new_value);
                Ok(())
            } else {
                let next = table
                    .get_mut(k)
                    .ok_or_else(|| format!("nested key {k:?} missing"))?;
                descend_set(next, tail, new_value)
            }
        }
        _ => Err("nested array indexing not supported in v1".into()),
    }
}
