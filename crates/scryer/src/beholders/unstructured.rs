//! Passthrough beholder — emits one `level: info` event per line.
//!
//! Lives at the tail of the registry as the "nothing else matched" fallback
//! per arch doc §"Service beholders". Matches every workload — that's the
//! whole point: nothing is silently dropped.

use observation::{Event, EventSource, Level};
use serde_json::Value;
use workload_spec::{ImageRef, MeshIdent};

use super::{
    BeholderCtx, LogLine, ServiceBeholder, ServiceBeholderFactory, ServiceHints,
};

const NAME: &str = "unstructured";
const VERSION: &str = "1";

pub struct UnstructuredBeholder {
    target: String,
}

impl UnstructuredBeholder {
    pub fn for_ident(ident: &MeshIdent) -> Self {
        Self { target: ident.0.clone() }
    }
}

impl ServiceBeholder for UnstructuredBeholder {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, _hints: &ServiceHints) -> bool {
        true
    }
    fn parse_line(&mut self, line: &LogLine, ctx: &mut BeholderCtx) -> Vec<Event> {
        if line.line.is_empty() {
            return Vec::new();
        }
        let event = ctx.make_event(
            line,
            Level::Info,
            self.target.clone(),
            line.line.clone(),
            Value::Object(Default::default()),
            EventSource::Beholder { name: NAME.to_string(), version: VERSION.to_string() },
        );
        vec![event]
    }
}

pub struct UnstructuredFactory;

impl ServiceBeholderFactory for UnstructuredFactory {
    fn name(&self) -> &'static str {
        NAME
    }
    fn version(&self) -> &'static str {
        VERSION
    }
    fn matches(&self, _ident: &MeshIdent, _image: &ImageRef, _hints: &ServiceHints) -> bool {
        true
    }
    fn create(&self) -> Box<dyn ServiceBeholder> {
        Box::new(UnstructuredBeholder { target: String::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> BeholderCtx {
        BeholderCtx::new()
    }

    #[test]
    fn unstructured_passthrough() {
        let mut b = UnstructuredBeholder::for_ident(&MeshIdent("api.pdx".to_string()));
        let mut c = ctx();
        let line = LogLine { line: "hello world".to_string(), offset_ms: 100 };
        let events = b.parse_line(&line, &mut c);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].msg, "hello world");
        assert_eq!(events[0].level, Level::Info);
        assert_eq!(events[0].target, "api.pdx");
        assert_eq!(events[0].offset_ms, 100);
    }

    #[test]
    fn unstructured_drops_empty_lines() {
        let mut b = UnstructuredBeholder::for_ident(&MeshIdent("api.pdx".to_string()));
        let mut c = ctx();
        let line = LogLine { line: String::new(), offset_ms: 0 };
        let events = b.parse_line(&line, &mut c);
        assert!(events.is_empty());
    }

    #[test]
    fn unstructured_seq_advances() {
        let mut b = UnstructuredBeholder::for_ident(&MeshIdent("api.pdx".to_string()));
        let mut c = ctx();
        for i in 0..3 {
            let line = LogLine { line: format!("line {i}"), offset_ms: i as u32 };
            let events = b.parse_line(&line, &mut c);
            assert_eq!(events[0].seq, i as u32);
        }
    }
}
