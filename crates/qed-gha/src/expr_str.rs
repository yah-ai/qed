//! `ExprString` — a scalar that may interleave literal text with `${{ … }}`
//! expression fragments.
//!
//! F1 only tokenizes: each `${{ … }}` block becomes an `Expr(raw)` token; the
//! text between blocks becomes `Literal(...)`. Evaluation is F2 — the raw
//! expression source is preserved verbatim so the F2 parser can take over
//! without re-reading YAML.

use std::fmt;

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ExprToken {
    Literal(String),
    Expr(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize)]
pub struct ExprString {
    pub tokens: Vec<ExprToken>,
}

impl ExprString {
    pub fn literal(s: impl Into<String>) -> Self {
        Self { tokens: vec![ExprToken::Literal(s.into())] }
    }

    /// True when the string carries no `${{ … }}` blocks.
    pub fn is_pure_literal(&self) -> bool {
        self.tokens.iter().all(|t| matches!(t, ExprToken::Literal(_)))
    }

    /// Concatenated literal text, ignoring any embedded expressions. Useful in
    /// F1 tests where the value happens to be all-literal.
    pub fn as_pure_literal(&self) -> Option<String> {
        let mut out = String::new();
        for t in &self.tokens {
            match t {
                ExprToken::Literal(s) => out.push_str(s),
                ExprToken::Expr(_) => return None,
            }
        }
        Some(out)
    }

    /// Parse a raw YAML scalar into a tokenized `ExprString`. Splits on every
    /// occurrence of `${{ … }}`; the inner text (trimmed) becomes an `Expr`
    /// token. Anything outside braces is a `Literal` token.
    pub fn parse(input: &str) -> Self {
        let mut tokens = Vec::new();
        let bytes = input.as_bytes();
        let mut i = 0usize;
        let mut last = 0usize;
        while i + 2 < bytes.len() {
            if bytes[i] == b'$' && bytes[i + 1] == b'{' && bytes[i + 2] == b'{' {
                let body_start = i + 3;
                if let Some(end) = find_expr_end(&input[body_start..]) {
                    if i > last {
                        tokens.push(ExprToken::Literal(input[last..i].to_string()));
                    }
                    let raw = &input[body_start..body_start + end];
                    tokens.push(ExprToken::Expr(raw.trim().to_string()));
                    i = body_start + end + 2; // skip `}}`
                    last = i;
                    continue;
                }
                // Unterminated `${{` — let the trailing tail emit the rest as
                // one literal token; don't split mid-stream.
                break;
            }
            i += 1;
        }
        if last < input.len() {
            tokens.push(ExprToken::Literal(input[last..].to_string()));
        }
        Self { tokens }
    }
}

/// Scan for the closing `}}` of a `${{ … }}` block. Returns the byte offset
/// of the `}}` within `body` (relative to body[0]).
///
/// F1 does not parse the expression — but it must skip over braces inside
/// string literals so a payload like `${{ format('{{}}', x) }}` doesn't fool
/// the splitter. Track single + double quote contexts; outside quotes, the
/// first `}}` closes the block.
fn find_expr_end(body: &str) -> Option<usize> {
    let bytes = body.as_bytes();
    let mut quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match quote {
            Some(q) => {
                // GHA expression strings use `''` to escape a single quote
                // inside a single-quoted literal. Same for `""`.
                if b == q {
                    if i + 1 < bytes.len() && bytes[i + 1] == q {
                        i += 2;
                        continue;
                    }
                    quote = None;
                }
                i += 1;
            }
            None => {
                if b == b'\'' || b == b'"' {
                    quote = Some(b);
                    i += 1;
                    continue;
                }
                if b == b'}' && i + 1 < bytes.len() && bytes[i + 1] == b'}' {
                    return Some(i);
                }
                i += 1;
            }
        }
    }
    None
}

impl<'de> Deserialize<'de> for ExprString {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        d.deserialize_any(ExprStringVisitor)
    }
}

struct ExprStringVisitor;

impl<'de> Visitor<'de> for ExprStringVisitor {
    type Value = ExprString;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a scalar (string, bool, integer, or float)")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(ExprString::parse(v))
    }

    fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(ExprString::parse(&v))
    }

    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Self::Value, E> {
        Ok(ExprString::literal(v.to_string()))
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
        Ok(ExprString::literal(v.to_string()))
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
        Ok(ExprString::literal(v.to_string()))
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Self::Value, E> {
        Ok(ExprString::literal(v.to_string()))
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ExprString::default())
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(ExprString::default())
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        ExprString::deserialize(d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pure_literal() {
        let s = ExprString::parse("hello world");
        assert_eq!(s.tokens, vec![ExprToken::Literal("hello world".into())]);
        assert!(s.is_pure_literal());
    }

    #[test]
    fn pure_expression() {
        let s = ExprString::parse("${{ github.ref_name }}");
        assert_eq!(s.tokens, vec![ExprToken::Expr("github.ref_name".into())]);
    }

    #[test]
    fn mixed() {
        let s = ExprString::parse("ghcr.io/yah-ai/yah-base:${{ github.ref_name }}");
        assert_eq!(
            s.tokens,
            vec![
                ExprToken::Literal("ghcr.io/yah-ai/yah-base:".into()),
                ExprToken::Expr("github.ref_name".into()),
            ]
        );
    }

    #[test]
    fn fallback_chain() {
        let s = ExprString::parse("${{ github.event.inputs.tag || github.ref_name }}");
        assert_eq!(
            s.tokens,
            vec![ExprToken::Expr("github.event.inputs.tag || github.ref_name".into())]
        );
    }

    #[test]
    fn braces_inside_string_literal_dont_close_block() {
        // `format('{0}', x)` contains `}` but it's inside a quoted string —
        // the splitter must skip it and find the real `}}` after the `)`.
        let s = ExprString::parse("${{ format('{0}', x) }}");
        assert_eq!(s.tokens, vec![ExprToken::Expr("format('{0}', x)".into())]);
    }

    #[test]
    fn two_expressions_separated_by_literal() {
        let s = ExprString::parse("${{ a }} and ${{ b }}");
        assert_eq!(
            s.tokens,
            vec![
                ExprToken::Expr("a".into()),
                ExprToken::Literal(" and ".into()),
                ExprToken::Expr("b".into()),
            ]
        );
    }

    #[test]
    fn yaml_scalars_normalize_to_literal() {
        let n: ExprString = serde_yaml::from_str("5").unwrap();
        assert_eq!(n.tokens, vec![ExprToken::Literal("5".into())]);
        let b: ExprString = serde_yaml::from_str("true").unwrap();
        assert_eq!(b.tokens, vec![ExprToken::Literal("true".into())]);
    }

    #[test]
    fn unterminated_expression_falls_back_to_literal_tail() {
        let s = ExprString::parse("prefix ${{ never_closed");
        assert_eq!(s.tokens, vec![ExprToken::Literal("prefix ${{ never_closed".into())]);
    }
}
