use serde::{Deserialize, Serialize};

/// Built-in shape vocabulary for typed pipeline outputs. Each variant
/// carries its own validation regex; type mismatch is a hard error before a
/// value can reach a bind target (W209 § Outputs).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ValueType {
    Blake3Hex,
    Sha256Hex,
    Semver,
    String,
    Uri,
    /// `sha256:<64-hex>` — OCI image digest shape.
    OciDigest,
    /// Escape hatch: blake3 of a deterministic-serialized directory tree
    /// (W209 § Coarse-grained outputs).
    DirBlake3,
}

impl ValueType {
    pub fn validate(self, value: &str) -> Result<(), String> {
        let ok = match self {
            ValueType::Blake3Hex | ValueType::DirBlake3 => {
                value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit())
            }
            ValueType::Sha256Hex => {
                value.len() == 64 && value.chars().all(|c| c.is_ascii_hexdigit())
            }
            ValueType::Semver => {
                // Loose check: <major>.<minor>.<patch> with optional
                // pre/build suffix. The semver crate could replace this
                // when a real predicate consumer needs strict parsing.
                let mut parts = value.splitn(3, '.');
                let major = parts.next();
                let minor = parts.next();
                let rest = parts.next();
                let digits = |s: Option<&str>| -> bool {
                    s.map(|x| !x.is_empty() && x.chars().all(|c| c.is_ascii_digit()))
                        .unwrap_or(false)
                };
                let patch_ok = rest.map(|r| {
                    let head: String = r.chars().take_while(|c| c.is_ascii_digit()).collect();
                    !head.is_empty()
                }).unwrap_or(false);
                digits(major) && digits(minor) && patch_ok
            }
            ValueType::String => !value.is_empty(),
            ValueType::Uri => value.contains("://") && !value.starts_with("://"),
            ValueType::OciDigest => value.starts_with("sha256:") && value.len() == 7 + 64,
        };
        if ok {
            Ok(())
        } else {
            Err(format!("value {value:?} does not match shape {self:?}"))
        }
    }
}
