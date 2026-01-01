//! GHA expression engine — parser + evaluator + [`Context`].
//!
//! Handles the operator/function/lookup surface used by `release.yml`:
//! `&&`, `||`, `==`, `!=`, `<`, `<=`, `>`, `>=`, unary `!`, parens,
//! literals (string/number/bool/null), dotted-path lookups
//! (`github.event.inputs.tag`, `needs.image-yah-base.outputs.digest`),
//! string-fallback `||` (`a || ''`), and the v1 function set:
//! `always`, `success`, `failure`, `cancelled`, `contains`, `startsWith`,
//! `endsWith`, `format`, `join`, `toJSON`, `fromJSON`, `hashFiles`.
//!
//! `hashFiles` is delegated to a host hook on [`Context`] so the evaluator
//! stays pure; the F2 stub returns `""` and is replaced by a real impl when
//! workflows that read the hash actually run (F4+).

use std::collections::HashMap;

use indexmap::IndexMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExprError {
    #[error("expr lex: {0}")]
    Lex(String),
    #[error("expr parse: {0}")]
    Parse(String),
    #[error("expr eval: {0}")]
    Eval(String),
}

// ─── Value ─────────────────────────────────────────────────────────────────

/// Evaluator value type. Mirrors the GHA contexts (which look JSON-shaped).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(IndexMap<String, Value>),
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Value {
    pub fn object() -> Self {
        Value::Object(IndexMap::new())
    }

    /// GHA truthiness: `null`, `false`, `0`, `""`, `NaN` are falsy.
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => *n != 0.0 && !n.is_nan(),
            Value::String(s) => !s.is_empty(),
            Value::Array(_) | Value::Object(_) => true,
        }
    }

    pub fn as_str_lossy(&self) -> String {
        match self {
            Value::Null => "".into(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() {
                    format!("{}", *n as i64)
                } else {
                    n.to_string()
                }
            }
            Value::String(s) => s.clone(),
            Value::Array(_) | Value::Object(_) => "".into(),
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.into())
    }
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}
impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}
impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Number(n)
    }
}
impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Number(n as f64)
    }
}

// ─── AST ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Literal(Value),
    /// `a.b.c` — segments are identifier names; lookup walks objects/arrays.
    Lookup(Vec<String>),
    Not(Box<Expr>),
    BinOp(BinOp, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

// ─── Tokenizer ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Number(f64),
    Str(String),
    True,
    False,
    Null,
    Dot,
    LParen,
    RParen,
    Comma,
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Bang,
}

fn tokenize(input: &str) -> Result<Vec<Tok>, ExprError> {
    let bytes = input.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        // Two-char operators first.
        if i + 1 < bytes.len() {
            let two = &input[i..i + 2];
            match two {
                "&&" => {
                    out.push(Tok::And);
                    i += 2;
                    continue;
                }
                "||" => {
                    out.push(Tok::Or);
                    i += 2;
                    continue;
                }
                "==" => {
                    out.push(Tok::Eq);
                    i += 2;
                    continue;
                }
                "!=" => {
                    out.push(Tok::Ne);
                    i += 2;
                    continue;
                }
                "<=" => {
                    out.push(Tok::Le);
                    i += 2;
                    continue;
                }
                ">=" => {
                    out.push(Tok::Ge);
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        match b {
            b'.' => {
                out.push(Tok::Dot);
                i += 1;
            }
            b'(' => {
                out.push(Tok::LParen);
                i += 1;
            }
            b')' => {
                out.push(Tok::RParen);
                i += 1;
            }
            b',' => {
                out.push(Tok::Comma);
                i += 1;
            }
            b'<' => {
                out.push(Tok::Lt);
                i += 1;
            }
            b'>' => {
                out.push(Tok::Gt);
                i += 1;
            }
            b'!' => {
                out.push(Tok::Bang);
                i += 1;
            }
            b'\'' => {
                // GHA single-quoted string; `''` escapes a `'`.
                let mut s = String::new();
                i += 1;
                loop {
                    if i >= bytes.len() {
                        return Err(ExprError::Lex("unterminated string literal".into()));
                    }
                    let c = bytes[i];
                    if c == b'\'' {
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            s.push('\'');
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    s.push(c as char);
                    i += 1;
                }
                out.push(Tok::Str(s));
            }
            c if c.is_ascii_digit() || (c == b'-' && peek_is_digit(bytes, i + 1)) => {
                let start = i;
                if bytes[i] == b'-' {
                    i += 1;
                }
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                    // Stop the trailing `.` from getting eaten if it's followed by a
                    // non-digit (member access). Only allow `.` once.
                    if bytes[i] == b'.'
                        && (input[start..i].contains('.')
                            || !peek_is_digit(bytes, i + 1))
                    {
                        break;
                    }
                    i += 1;
                }
                let n: f64 = input[start..i]
                    .parse()
                    .map_err(|e| ExprError::Lex(format!("bad number `{}`: {e}", &input[start..i])))?;
                out.push(Tok::Number(n));
            }
            c if is_ident_start(c) => {
                let start = i;
                while i < bytes.len() && is_ident_cont(bytes[i]) {
                    i += 1;
                }
                let word = &input[start..i];
                match word {
                    "true" => out.push(Tok::True),
                    "false" => out.push(Tok::False),
                    "null" => out.push(Tok::Null),
                    _ => out.push(Tok::Ident(word.to_string())),
                }
            }
            _ => {
                return Err(ExprError::Lex(format!(
                    "unexpected byte {:?} at offset {i}",
                    b as char
                )))
            }
        }
    }
    Ok(out)
}

fn peek_is_digit(b: &[u8], i: usize) -> bool {
    i < b.len() && b[i].is_ascii_digit()
}

fn is_ident_start(c: u8) -> bool {
    c.is_ascii_alphabetic() || c == b'_'
}

/// Identifier continuation allows `-` so dotted-path segments can match GHA
/// job IDs like `image-yah-base`.
fn is_ident_cont(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'-'
}

// ─── Parser ────────────────────────────────────────────────────────────────

/// Parse an expression string. Strips an optional leading `${{` and trailing
/// `}}` so callers can hand over raw scalar text without pre-trimming.
pub fn parse(input: &str) -> Result<Expr, ExprError> {
    let body = strip_braces(input.trim()).trim();
    let toks = tokenize(body)?;
    let mut p = Parser { toks, pos: 0 };
    let e = p.parse_or()?;
    if p.pos != p.toks.len() {
        return Err(ExprError::Parse(format!(
            "trailing tokens at position {}",
            p.pos
        )));
    }
    Ok(e)
}

fn strip_braces(s: &str) -> &str {
    if let Some(inner) = s.strip_prefix("${{") {
        if let Some(inner) = inner.strip_suffix("}}") {
            return inner;
        }
    }
    s
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn bump(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }
    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    // or := and ('||' and)*
    fn parse_or(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_and()?;
        while self.eat(&Tok::Or) {
            let right = self.parse_and()?;
            left = Expr::BinOp(BinOp::Or, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // and := cmp ('&&' cmp)*
    fn parse_and(&mut self) -> Result<Expr, ExprError> {
        let mut left = self.parse_cmp()?;
        while self.eat(&Tok::And) {
            let right = self.parse_cmp()?;
            left = Expr::BinOp(BinOp::And, Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // cmp := unary ((== | != | < | <= | > | >=) unary)?
    fn parse_cmp(&mut self) -> Result<Expr, ExprError> {
        let left = self.parse_unary()?;
        let op = match self.peek() {
            Some(Tok::Eq) => Some(BinOp::Eq),
            Some(Tok::Ne) => Some(BinOp::Ne),
            Some(Tok::Lt) => Some(BinOp::Lt),
            Some(Tok::Le) => Some(BinOp::Le),
            Some(Tok::Gt) => Some(BinOp::Gt),
            Some(Tok::Ge) => Some(BinOp::Ge),
            _ => None,
        };
        if let Some(op) = op {
            self.pos += 1;
            let right = self.parse_unary()?;
            return Ok(Expr::BinOp(op, Box::new(left), Box::new(right)));
        }
        Ok(left)
    }

    // unary := '!' unary | primary
    fn parse_unary(&mut self) -> Result<Expr, ExprError> {
        if self.eat(&Tok::Bang) {
            let e = self.parse_unary()?;
            return Ok(Expr::Not(Box::new(e)));
        }
        self.parse_primary()
    }

    // primary := '(' or ')'
    //          | literal
    //          | ident ( '(' args ')' )? ( '.' ident )*
    fn parse_primary(&mut self) -> Result<Expr, ExprError> {
        match self.bump() {
            Some(Tok::LParen) => {
                let e = self.parse_or()?;
                if !self.eat(&Tok::RParen) {
                    return Err(ExprError::Parse("expected `)`".into()));
                }
                Ok(e)
            }
            Some(Tok::Number(n)) => Ok(Expr::Literal(Value::Number(n))),
            Some(Tok::Str(s)) => Ok(Expr::Literal(Value::String(s))),
            Some(Tok::True) => Ok(Expr::Literal(Value::Bool(true))),
            Some(Tok::False) => Ok(Expr::Literal(Value::Bool(false))),
            Some(Tok::Null) => Ok(Expr::Literal(Value::Null)),
            Some(Tok::Ident(name)) => {
                if self.eat(&Tok::LParen) {
                    let mut args = Vec::new();
                    if !matches!(self.peek(), Some(Tok::RParen)) {
                        loop {
                            args.push(self.parse_or()?);
                            if !self.eat(&Tok::Comma) {
                                break;
                            }
                        }
                    }
                    if !self.eat(&Tok::RParen) {
                        return Err(ExprError::Parse(format!("expected `)` after `{name}(`")));
                    }
                    Ok(Expr::Call(name, args))
                } else {
                    let mut path = vec![name];
                    while self.eat(&Tok::Dot) {
                        match self.bump() {
                            Some(Tok::Ident(s)) => path.push(s),
                            // GHA allows numeric indexes via `.0.1` — accept
                            // them as path segments so `needs.foo.outputs.0`
                            // parses, even though we don't use that today.
                            Some(Tok::Number(n)) => {
                                if n.fract() == 0.0 && n >= 0.0 {
                                    path.push((n as i64).to_string());
                                } else {
                                    return Err(ExprError::Parse(
                                        "non-integer index after `.`".into(),
                                    ));
                                }
                            }
                            other => {
                                return Err(ExprError::Parse(format!(
                                    "expected identifier after `.`, got {other:?}"
                                )))
                            }
                        }
                    }
                    Ok(Expr::Lookup(path))
                }
            }
            other => Err(ExprError::Parse(format!(
                "unexpected token {other:?} at primary"
            ))),
        }
    }
}

// ─── Context ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Success,
    Failure,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JobStatus::Success => "success",
            JobStatus::Failure => "failure",
            JobStatus::Cancelled => "cancelled",
        }
    }
}

/// Evaluator context. All namespaces are [`Value::Object`]s the lookup walker
/// can descend into. F2 does not enforce a schema on them — callers (F3+)
/// build the objects from job state at scheduling time.
#[derive(Default)]
pub struct Context<'a> {
    pub github: Value,
    pub env: Value,
    pub vars: Value,
    pub matrix: Option<Value>,
    pub needs: Value,
    pub steps: Value,
    pub inputs: Value,
    pub runner: Value,
    pub secrets: Value,
    pub job: Value,
    /// Optional override of `always()`/`success()`/`failure()`/`cancelled()`
    /// behavior. Defaults to `Success`.
    pub job_status: Option<JobStatus>,
    /// Host hook for `hashFiles(...)`. Defaults to returning `""` — that's
    /// good enough for parse + dry-run; F4+ wires this to a real walker.
    pub hash_files: Option<&'a dyn Fn(&[String]) -> String>,
}

impl<'a> Context<'a> {
    pub fn new() -> Self {
        let mut c = Self::default();
        // Always provide an empty object so `runner.os` returns null rather than
        // erroring on the first dotted lookup.
        c.github = Value::object();
        c.env = Value::object();
        c.vars = Value::object();
        c.needs = Value::object();
        c.steps = Value::object();
        c.inputs = Value::object();
        c.runner = Value::object();
        c.secrets = Value::object();
        c.job = Value::object();
        c
    }

    fn root(&self, name: &str) -> Option<&Value> {
        match name {
            "github" => Some(&self.github),
            "env" => Some(&self.env),
            "vars" => Some(&self.vars),
            "matrix" => self.matrix.as_ref(),
            "needs" => Some(&self.needs),
            "steps" => Some(&self.steps),
            "inputs" => Some(&self.inputs),
            "runner" => Some(&self.runner),
            "secrets" => Some(&self.secrets),
            "job" => Some(&self.job),
            _ => None,
        }
    }
}

// ─── Evaluator ─────────────────────────────────────────────────────────────

pub fn eval(expr: &Expr, ctx: &Context) -> Result<Value, ExprError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Lookup(path) => Ok(lookup(ctx, path)),
        Expr::Not(inner) => {
            let v = eval(inner, ctx)?;
            Ok(Value::Bool(!v.is_truthy()))
        }
        Expr::BinOp(op, l, r) => {
            let lv = eval(l, ctx)?;
            match op {
                // Short-circuit + value-preserving (GHA semantics).
                BinOp::And => {
                    if !lv.is_truthy() {
                        Ok(lv)
                    } else {
                        eval(r, ctx)
                    }
                }
                BinOp::Or => {
                    if lv.is_truthy() {
                        Ok(lv)
                    } else {
                        eval(r, ctx)
                    }
                }
                _ => {
                    let rv = eval(r, ctx)?;
                    Ok(Value::Bool(compare(*op, &lv, &rv)))
                }
            }
        }
        Expr::Call(name, args) => call(name, args, ctx),
    }
}

/// Convenience: parse + eval an expression body in one go.
pub fn evaluate(input: &str, ctx: &Context) -> Result<Value, ExprError> {
    let e = parse(input)?;
    eval(&e, ctx)
}

fn lookup(ctx: &Context, path: &[String]) -> Value {
    let Some((root, rest)) = path.split_first() else {
        return Value::Null;
    };
    let mut cur = match ctx.root(root) {
        Some(v) => v.clone(),
        None => return Value::Null,
    };
    for seg in rest {
        cur = match cur {
            Value::Object(map) => map.get(seg).cloned().unwrap_or(Value::Null),
            Value::Array(arr) => match seg.parse::<usize>() {
                Ok(idx) => arr.get(idx).cloned().unwrap_or(Value::Null),
                Err(_) => Value::Null,
            },
            _ => Value::Null,
        };
        if cur == Value::Null {
            return Value::Null;
        }
    }
    cur
}

fn compare(op: BinOp, l: &Value, r: &Value) -> bool {
    // GHA coerces across types for ==/!=. Best-effort match:
    //   - same kind → direct compare
    //   - bool vs anything → coerce to bool truthiness
    //   - number vs string → parse string as number
    //   - null vs null → eq; null vs anything else → ne
    let (eq, ord) = compare_kinds(l, r);
    match op {
        BinOp::Eq => eq,
        BinOp::Ne => !eq,
        BinOp::Lt => matches!(ord, Some(std::cmp::Ordering::Less)),
        BinOp::Le => matches!(ord, Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)),
        BinOp::Gt => matches!(ord, Some(std::cmp::Ordering::Greater)),
        BinOp::Ge => matches!(
            ord,
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        BinOp::And | BinOp::Or => unreachable!("handled in eval()"),
    }
}

fn compare_kinds(l: &Value, r: &Value) -> (bool, Option<std::cmp::Ordering>) {
    use Value::*;
    match (l, r) {
        (Null, Null) => (true, Some(std::cmp::Ordering::Equal)),
        (Null, _) | (_, Null) => (false, None),
        (Bool(a), Bool(b)) => (a == b, Some(a.cmp(b))),
        (Number(a), Number(b)) => (a == b, a.partial_cmp(b)),
        (String(a), String(b)) => (a == b, Some(a.cmp(b))),
        // GHA: comparing number-or-bool to a string parses the string as a
        // number; if parse fails the comparison is `false`.
        (Number(a), String(s)) | (String(s), Number(a)) => match s.parse::<f64>() {
            Ok(b) => (*a == b, a.partial_cmp(&b)),
            Err(_) => (false, None),
        },
        (Bool(a), Number(b)) | (Number(b), Bool(a)) => {
            let an = if *a { 1.0_f64 } else { 0.0_f64 };
            (an == *b, an.partial_cmp(b))
        }
        (Bool(a), String(s)) | (String(s), Bool(a)) => {
            let bs = if *a { "true" } else { "false" };
            (bs == s, Some(bs.cmp(s.as_str())))
        }
        // Object/Array equality is identity-ish; fall back to false. Ordering
        // is undefined.
        _ => (false, None),
    }
}

fn call(name: &str, args: &[Expr], ctx: &Context) -> Result<Value, ExprError> {
    let want = |n: usize| -> Result<(), ExprError> {
        if args.len() != n {
            return Err(ExprError::Eval(format!(
                "function `{name}` expects {n} args, got {}",
                args.len()
            )));
        }
        Ok(())
    };
    match name {
        // Status fns: `always()` is true regardless; the others depend on
        // the current job's aggregated status.
        "always" => {
            want(0)?;
            Ok(Value::Bool(true))
        }
        "success" => {
            want(0)?;
            Ok(Value::Bool(
                ctx.job_status.unwrap_or(JobStatus::Success) == JobStatus::Success,
            ))
        }
        "failure" => {
            want(0)?;
            Ok(Value::Bool(
                ctx.job_status.unwrap_or(JobStatus::Success) == JobStatus::Failure,
            ))
        }
        "cancelled" => {
            want(0)?;
            Ok(Value::Bool(
                ctx.job_status.unwrap_or(JobStatus::Success) == JobStatus::Cancelled,
            ))
        }
        // `contains(search, item)` — string-in-string or item-in-array.
        "contains" => {
            want(2)?;
            let a = eval(&args[0], ctx)?;
            let b = eval(&args[1], ctx)?;
            let result = match (&a, &b) {
                (Value::String(hay), Value::String(needle)) => hay.contains(needle.as_str()),
                (Value::Array(arr), needle) => arr.iter().any(|v| compare_kinds(v, needle).0),
                (Value::String(hay), other) => hay.contains(&other.as_str_lossy()),
                _ => false,
            };
            Ok(Value::Bool(result))
        }
        "startsWith" => {
            want(2)?;
            let a = eval(&args[0], ctx)?.as_str_lossy();
            let b = eval(&args[1], ctx)?.as_str_lossy();
            Ok(Value::Bool(a.starts_with(&b)))
        }
        "endsWith" => {
            want(2)?;
            let a = eval(&args[0], ctx)?.as_str_lossy();
            let b = eval(&args[1], ctx)?.as_str_lossy();
            Ok(Value::Bool(a.ends_with(&b)))
        }
        // `format('{0}-{1}', a, b)` — GHA positional placeholders. `{{`/`}}`
        // escape literal braces.
        "format" => {
            if args.is_empty() {
                return Err(ExprError::Eval("format() needs at least a template".into()));
            }
            let template = eval(&args[0], ctx)?.as_str_lossy();
            let mut params = Vec::with_capacity(args.len() - 1);
            for arg in &args[1..] {
                params.push(eval(arg, ctx)?);
            }
            Ok(Value::String(format_gha(&template, &params)?))
        }
        "join" => {
            // join(array) or join(array, sep)
            if args.is_empty() || args.len() > 2 {
                return Err(ExprError::Eval("join() takes 1 or 2 args".into()));
            }
            let arr = eval(&args[0], ctx)?;
            let sep = if args.len() == 2 {
                eval(&args[1], ctx)?.as_str_lossy()
            } else {
                ",".into()
            };
            let parts: Vec<String> = match arr {
                Value::Array(a) => a.iter().map(|v| v.as_str_lossy()).collect(),
                other => vec![other.as_str_lossy()],
            };
            Ok(Value::String(parts.join(&sep)))
        }
        "toJSON" => {
            want(1)?;
            let v = eval(&args[0], ctx)?;
            Ok(Value::String(value_to_json(&v)))
        }
        "fromJSON" => {
            want(1)?;
            let s = eval(&args[0], ctx)?.as_str_lossy();
            json_to_value(&s)
                .map_err(|e| ExprError::Eval(format!("fromJSON: {e}")))
        }
        "hashFiles" => {
            let mut globs = Vec::with_capacity(args.len());
            for a in args {
                globs.push(eval(a, ctx)?.as_str_lossy());
            }
            let out = match ctx.hash_files {
                Some(f) => f(&globs),
                None => String::new(),
            };
            Ok(Value::String(out))
        }
        _ => Err(ExprError::Eval(format!("unknown function `{name}`"))),
    }
}

/// Minimal GHA-style `format()`: replace `{N}` with the Nth arg's string form.
/// `{{` / `}}` escape literal braces.
fn format_gha(template: &str, params: &[Value]) -> Result<String, ExprError> {
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'{' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                out.push('{');
                i += 2;
                continue;
            }
            // Read index until `}`.
            let start = i + 1;
            let mut j = start;
            while j < bytes.len() && bytes[j] != b'}' {
                j += 1;
            }
            if j == bytes.len() {
                return Err(ExprError::Eval("unterminated `{` in format()".into()));
            }
            let idx: usize = template[start..j]
                .parse()
                .map_err(|e| ExprError::Eval(format!("bad format index: {e}")))?;
            let v = params
                .get(idx)
                .ok_or_else(|| ExprError::Eval(format!("format index {idx} out of range")))?;
            out.push_str(&v.as_str_lossy());
            i = j + 1;
            continue;
        }
        if c == b'}' && i + 1 < bytes.len() && bytes[i + 1] == b'}' {
            out.push('}');
            i += 2;
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    Ok(out)
}

// ─── JSON in/out (for toJSON / fromJSON) ───────────────────────────────────
//
// Hand-rolled so we don't pull a real JSON crate just for these two fns; the
// shapes that actually appear in workflows are leaf-scalar arrays/objects.

fn value_to_json(v: &Value) -> String {
    let mut out = String::new();
    write_json(v, &mut out);
    out
}

fn write_json(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() {
                out.push_str(&format!("{}", *n as i64));
            } else {
                out.push_str(&n.to_string());
            }
        }
        Value::String(s) => {
            out.push('"');
            for c in s.chars() {
                match c {
                    '"' => out.push_str("\\\""),
                    '\\' => out.push_str("\\\\"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\t' => out.push_str("\\t"),
                    c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
                    c => out.push(c),
                }
            }
            out.push('"');
        }
        Value::Array(a) => {
            out.push('[');
            for (i, item) in a.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (k, val)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json(&Value::String(k.clone()), out);
                out.push(':');
                write_json(val, out);
            }
            out.push('}');
        }
    }
}

fn json_to_value(s: &str) -> Result<Value, String> {
    let bytes = s.as_bytes();
    let mut p = JsonP { bytes, i: 0 };
    p.skip_ws();
    let v = p.parse()?;
    p.skip_ws();
    if p.i != bytes.len() {
        return Err(format!("trailing JSON at offset {}", p.i));
    }
    Ok(v)
}

struct JsonP<'a> {
    bytes: &'a [u8],
    i: usize,
}

impl<'a> JsonP<'a> {
    fn skip_ws(&mut self) {
        while self.i < self.bytes.len() && self.bytes[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn parse(&mut self) -> Result<Value, String> {
        self.skip_ws();
        if self.i >= self.bytes.len() {
            return Err("unexpected eof".into());
        }
        match self.bytes[self.i] {
            b'n' => self.lit("null", Value::Null),
            b't' => self.lit("true", Value::Bool(true)),
            b'f' => self.lit("false", Value::Bool(false)),
            b'"' => self.parse_string().map(Value::String),
            b'[' => self.parse_array(),
            b'{' => self.parse_object(),
            _ => self.parse_number(),
        }
    }
    fn lit(&mut self, kw: &str, out: Value) -> Result<Value, String> {
        let end = self.i + kw.len();
        if end > self.bytes.len() || &self.bytes[self.i..end] != kw.as_bytes() {
            return Err(format!("expected `{kw}` at {}", self.i));
        }
        self.i = end;
        Ok(out)
    }
    fn parse_string(&mut self) -> Result<String, String> {
        self.i += 1; // opening "
        let mut out = String::new();
        while self.i < self.bytes.len() {
            let c = self.bytes[self.i];
            if c == b'"' {
                self.i += 1;
                return Ok(out);
            }
            if c == b'\\' {
                self.i += 1;
                if self.i >= self.bytes.len() {
                    return Err("trailing `\\`".into());
                }
                match self.bytes[self.i] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b => out.push(b as char),
                }
                self.i += 1;
                continue;
            }
            out.push(c as char);
            self.i += 1;
        }
        Err("unterminated string".into())
    }
    fn parse_array(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut out = Vec::new();
        self.skip_ws();
        if self.i < self.bytes.len() && self.bytes[self.i] == b']' {
            self.i += 1;
            return Ok(Value::Array(out));
        }
        loop {
            out.push(self.parse()?);
            self.skip_ws();
            match self.bytes.get(self.i).copied() {
                Some(b',') => {
                    self.i += 1;
                    self.skip_ws();
                }
                Some(b']') => {
                    self.i += 1;
                    return Ok(Value::Array(out));
                }
                _ => return Err("expected `,` or `]`".into()),
            }
        }
    }
    fn parse_object(&mut self) -> Result<Value, String> {
        self.i += 1;
        let mut out = IndexMap::new();
        self.skip_ws();
        if self.i < self.bytes.len() && self.bytes[self.i] == b'}' {
            self.i += 1;
            return Ok(Value::Object(out));
        }
        loop {
            self.skip_ws();
            if self.bytes.get(self.i).copied() != Some(b'"') {
                return Err("expected string key".into());
            }
            let k = self.parse_string()?;
            self.skip_ws();
            if self.bytes.get(self.i).copied() != Some(b':') {
                return Err("expected `:` after key".into());
            }
            self.i += 1;
            let v = self.parse()?;
            out.insert(k, v);
            self.skip_ws();
            match self.bytes.get(self.i).copied() {
                Some(b',') => {
                    self.i += 1;
                    self.skip_ws();
                }
                Some(b'}') => {
                    self.i += 1;
                    return Ok(Value::Object(out));
                }
                _ => return Err("expected `,` or `}`".into()),
            }
        }
    }
    fn parse_number(&mut self) -> Result<Value, String> {
        let start = self.i;
        if self.bytes[self.i] == b'-' {
            self.i += 1;
        }
        while self.i < self.bytes.len()
            && (self.bytes[self.i].is_ascii_digit()
                || self.bytes[self.i] == b'.'
                || self.bytes[self.i] == b'e'
                || self.bytes[self.i] == b'E'
                || self.bytes[self.i] == b'+'
                || self.bytes[self.i] == b'-')
        {
            self.i += 1;
        }
        let s = std::str::from_utf8(&self.bytes[start..self.i]).map_err(|e| e.to_string())?;
        s.parse::<f64>().map(Value::Number).map_err(|e| e.to_string())
    }
}

// ─── Test fixtures ─────────────────────────────────────────────────────────

/// Build a [`Value::Object`] from a slice of `(key, value)` pairs. Helper
/// for tests + downstream phases that synthesize contexts.
pub fn obj<I, K, V>(entries: I) -> Value
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<Value>,
{
    let mut map = IndexMap::new();
    for (k, v) in entries {
        map.insert(k.into(), v.into());
    }
    Value::Object(map)
}

/// Pull a (possibly empty) `Object`'s entries out for inspection. Test-only
/// convenience; not part of the public surface yet.
#[allow(dead_code)]
fn flat<'a>(v: &'a Value) -> &'a IndexMap<String, Value> {
    if let Value::Object(m) = v {
        m
    } else {
        // SAFETY-ish: only called from tests with object values.
        panic!("flat() expected Object, got {v:?}");
    }
}

// Silence the `HashMap` import (used in tests below).
#[allow(dead_code)]
fn _hm() -> HashMap<String, Value> {
    HashMap::new()
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(v.into())
    }
    fn b(v: bool) -> Value {
        Value::Bool(v)
    }

    fn ctx() -> Context<'static> {
        Context::new()
    }

    fn ev(expr: &str, c: &Context) -> Value {
        evaluate(expr, c).unwrap_or_else(|e| panic!("eval `{expr}`: {e}"))
    }

    // ── tokenizer / parser primitives

    #[test]
    fn tokenize_basic_ops() {
        let ts = tokenize("a && b || c == d != e < f <= g > h >= i").unwrap();
        // Spot-check the operator tokens land in order.
        let ops: Vec<_> = ts
            .iter()
            .filter(|t| {
                !matches!(
                    t,
                    Tok::Ident(_) | Tok::Number(_) | Tok::Str(_) | Tok::True | Tok::False | Tok::Null
                )
            })
            .collect();
        assert_eq!(
            ops,
            vec![&Tok::And, &Tok::Or, &Tok::Eq, &Tok::Ne, &Tok::Lt, &Tok::Le, &Tok::Gt, &Tok::Ge]
        );
    }

    #[test]
    fn tokenize_string_with_escaped_quote() {
        let ts = tokenize("'it''s ok'").unwrap();
        assert_eq!(ts, vec![Tok::Str("it's ok".into())]);
    }

    #[test]
    fn parse_strips_braces() {
        // strip + parse the embedded form.
        let e = parse("${{ github.ref_name }}").unwrap();
        assert_eq!(e, Expr::Lookup(vec!["github".into(), "ref_name".into()]));
    }

    #[test]
    fn parse_identifier_with_hyphen() {
        let e = parse("needs.image-yah-base.outputs.digest").unwrap();
        assert_eq!(
            e,
            Expr::Lookup(vec![
                "needs".into(),
                "image-yah-base".into(),
                "outputs".into(),
                "digest".into(),
            ])
        );
    }

    // ── precedence: && binds tighter than ||

    #[test]
    fn or_lower_precedence_than_and() {
        // a || b && c   ==   a || (b && c)
        let e = parse("a || b && c").unwrap();
        let expected = Expr::BinOp(
            BinOp::Or,
            Box::new(Expr::Lookup(vec!["a".into()])),
            Box::new(Expr::BinOp(
                BinOp::And,
                Box::new(Expr::Lookup(vec!["b".into()])),
                Box::new(Expr::Lookup(vec!["c".into()])),
            )),
        );
        assert_eq!(e, expected);
    }

    // ── lookup against context

    #[test]
    fn lookup_dotted_path() {
        let mut c = ctx();
        c.github = obj([("ref_name", s("v1.2.3"))]);
        assert_eq!(ev("github.ref_name", &c), s("v1.2.3"));
        assert_eq!(ev("github.nonexistent", &c), Value::Null);
    }

    #[test]
    fn lookup_nested_outputs() {
        // `needs.image-yah-base.outputs.digest` is the audit's bread-and-butter shape.
        let mut c = ctx();
        c.needs = obj([(
            "image-yah-base",
            obj([("outputs", obj([("digest", s("sha256:abc"))]))]),
        )]);
        assert_eq!(ev("needs.image-yah-base.outputs.digest", &c), s("sha256:abc"));
    }

    #[test]
    fn matrix_optional_returns_null_when_unset() {
        let c = ctx();
        assert_eq!(ev("matrix.os", &c), Value::Null);
    }

    // ── operators with GHA semantics

    #[test]
    fn or_short_circuits_to_string_fallback() {
        // matrix.use_target_flag && matrix.target || ''
        let mut c = ctx();
        c.matrix = Some(obj([("use_target_flag", b(false)), ("target", s("x86"))]));
        let v = ev("matrix.use_target_flag && matrix.target || ''", &c);
        assert_eq!(v, s(""));

        c.matrix = Some(obj([("use_target_flag", b(true)), ("target", s("x86"))]));
        let v = ev("matrix.use_target_flag && matrix.target || ''", &c);
        assert_eq!(v, s("x86"));
    }

    #[test]
    fn or_preserves_left_truthy_value() {
        // github.event.inputs.tag || github.ref_name
        let mut c = ctx();
        c.github = obj([
            ("event", obj([("inputs", obj([("tag", s("v0.0.1-test"))]))])),
            ("ref_name", s("v0.5.0")),
        ]);
        assert_eq!(
            ev("github.event.inputs.tag || github.ref_name", &c),
            s("v0.0.1-test")
        );

        // Fall through when left is null.
        c.github = obj([("ref_name", s("v0.5.0"))]);
        assert_eq!(
            ev("github.event.inputs.tag || github.ref_name", &c),
            s("v0.5.0")
        );
    }

    #[test]
    fn eq_and_neq_with_strings() {
        let mut c = ctx();
        c.github = obj([("event_name", s("push"))]);
        assert!(ev("github.event_name == 'push'", &c).is_truthy());
        assert!(!ev("github.event_name == 'workflow_dispatch'", &c).is_truthy());
        assert!(ev("github.event_name != 'workflow_dispatch'", &c).is_truthy());
    }

    #[test]
    fn unary_not_negates_truthiness() {
        let mut c = ctx();
        c.github = obj([("ref_name", s("v1.2.3-rc1"))]);
        assert!(ev("contains(github.ref_name, '-')", &c).is_truthy());
        assert!(!ev("!contains(github.ref_name, '-')", &c).is_truthy());
    }

    // ── functions

    #[test]
    fn contains_string() {
        let c = ctx();
        assert!(ev("contains('hello world', 'world')", &c).is_truthy());
        assert!(!ev("contains('hello', 'xyz')", &c).is_truthy());
    }

    #[test]
    fn contains_array() {
        let mut c = ctx();
        c.matrix = Some(obj([(
            "targets",
            Value::Array(vec![s("linux"), s("macos")]),
        )]));
        assert!(ev("contains(matrix.targets, 'linux')", &c).is_truthy());
        assert!(!ev("contains(matrix.targets, 'windows')", &c).is_truthy());
    }

    #[test]
    fn status_functions_track_job_status() {
        let mut c = ctx();
        c.job_status = Some(JobStatus::Success);
        assert!(ev("always()", &c).is_truthy());
        assert!(ev("success()", &c).is_truthy());
        assert!(!ev("failure()", &c).is_truthy());
        c.job_status = Some(JobStatus::Failure);
        assert!(ev("always()", &c).is_truthy());
        assert!(!ev("success()", &c).is_truthy());
        assert!(ev("failure()", &c).is_truthy());
        c.job_status = Some(JobStatus::Cancelled);
        assert!(ev("cancelled()", &c).is_truthy());
    }

    #[test]
    fn format_replaces_indexed_holes() {
        let c = ctx();
        assert_eq!(
            ev("format('{0}-{1}', 'a', 'b')", &c),
            s("a-b")
        );
        // {{ }} escape literal braces.
        assert_eq!(ev("format('{{{0}}}', 'x')", &c), s("{x}"));
    }

    #[test]
    fn hashfiles_uses_host_hook() {
        fn hook(globs: &[String]) -> String {
            format!("hash:{}", globs.join("+"))
        }
        let mut c = ctx();
        c.hash_files = Some(&hook);
        assert_eq!(
            ev("hashFiles('crates/yah/cloud/worker/bun.lock')", &c),
            s("hash:crates/yah/cloud/worker/bun.lock")
        );
    }

    #[test]
    fn tojson_and_fromjson_roundtrip() {
        let mut c = ctx();
        c.inputs = obj([("tag", s("v1")), ("skip", b(true))]);
        let j = ev("toJSON(inputs)", &c);
        let s = j.as_str_lossy();
        assert!(s.contains("\"tag\":\"v1\""));
        let back = evaluate(&format!("fromJSON('{}')", s.replace('\'', "''")), &c).unwrap();
        if let Value::Object(m) = back {
            assert_eq!(m.get("tag"), Some(&Value::String("v1".into())));
            assert_eq!(m.get("skip"), Some(&Value::Bool(true)));
        } else {
            panic!("expected object, got {back:?}");
        }
    }

    // ── audit-table coverage

    #[test]
    fn release_yml_smoke_if() {
        // (github.event_name == 'push' && !contains(github.ref_name, '-')) ||
        // (github.event_name == 'workflow_dispatch' && inputs.skip_smoke != true)
        let expr = "(github.event_name == 'push' && !contains(github.ref_name, '-')) || \
                    (github.event_name == 'workflow_dispatch' && inputs.skip_smoke != true)";
        let mut c = ctx();

        // Final-SemVer tag pushed via push event → smoke runs.
        c.github = obj([("event_name", s("push")), ("ref_name", s("v1.2.3"))]);
        c.inputs = obj::<Vec<(String, Value)>, _, _>(vec![]);
        assert!(ev(expr, &c).is_truthy(), "push v1.2.3 should run");

        // RC tag → skip.
        c.github = obj([("event_name", s("push")), ("ref_name", s("v1.2.3-rc1"))]);
        assert!(!ev(expr, &c).is_truthy(), "push v1.2.3-rc1 should skip");

        // workflow_dispatch with skip_smoke=true → skip.
        c.github = obj([("event_name", s("workflow_dispatch")), ("ref_name", s("v1"))]);
        c.inputs = obj([("skip_smoke", b(true))]);
        assert!(!ev(expr, &c).is_truthy(), "skip_smoke=true should skip");

        // workflow_dispatch with skip_smoke=false → run.
        c.inputs = obj([("skip_smoke", b(false))]);
        assert!(ev(expr, &c).is_truthy(), "skip_smoke=false should run");
    }

    #[test]
    fn release_yml_image_gate() {
        // always() && needs.smoke.result != 'failure' && needs.smoke.result != 'cancelled'
        let expr = "always() && needs.smoke.result != 'failure' && needs.smoke.result != 'cancelled'";
        let mut c = ctx();

        c.needs = obj([("smoke", obj([("result", s("success"))]))]);
        assert!(ev(expr, &c).is_truthy());

        c.needs = obj([("smoke", obj([("result", s("skipped"))]))]);
        assert!(ev(expr, &c).is_truthy(), "skipped is fine, only failure/cancelled gate");

        c.needs = obj([("smoke", obj([("result", s("failure"))]))]);
        assert!(!ev(expr, &c).is_truthy());

        c.needs = obj([("smoke", obj([("result", s("cancelled"))]))]);
        assert!(!ev(expr, &c).is_truthy());
    }

    #[test]
    fn release_yml_image_dep_gate() {
        // always() && needs.image-yah-rust.result == 'success'
        let expr = "always() && needs.image-yah-rust.result == 'success'";
        let mut c = ctx();
        c.needs = obj([("image-yah-rust", obj([("result", s("success"))]))]);
        assert!(ev(expr, &c).is_truthy());
        c.needs = obj([("image-yah-rust", obj([("result", s("failure"))]))]);
        assert!(!ev(expr, &c).is_truthy());
    }

    #[test]
    fn release_yml_image_tag_template() {
        // Mixed template — the ExprString-level value `ghcr.io/.../yah-base:${{ ... }}`
        // is composed by callers; here we just verify the embedded expression evaluates.
        let mut c = ctx();
        c.github = obj([
            ("event", obj([("inputs", obj([("tag", s("v0.0.1-test"))]))])),
            ("ref_name", s("v0.5.0")),
        ]);
        let v = ev("github.event.inputs.tag || github.ref_name", &c);
        assert_eq!(v, s("v0.0.1-test"));
    }

    #[test]
    fn smoke_yml_panic_template() {
        // ${{ inputs.induce_panic == true && '1' || '' }}
        let expr = "inputs.induce_panic == true && '1' || ''";
        let mut c = ctx();
        c.inputs = obj([("induce_panic", b(true))]);
        assert_eq!(ev(expr, &c), s("1"));
        c.inputs = obj([("induce_panic", b(false))]);
        assert_eq!(ev(expr, &c), s(""));
    }

    #[test]
    fn ci_yml_workspace_runs_on_matrix_os() {
        // ${{ matrix.os }} — pure lookup.
        let mut c = ctx();
        c.matrix = Some(obj([("os", s("ubuntu-latest"))]));
        assert_eq!(ev("matrix.os", &c), s("ubuntu-latest"));
    }
}
