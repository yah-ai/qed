//! Shared Sparkle/WinSparkle appcast builder (R509-F3, reused by R509-F4).
//!
//! Both the `sparkle` (macOS) and `winsparkle` (Windows) adapters publish a
//! Sparkle-shaped **appcast** — an RSS 2.0 feed whose `<item>`s carry an
//! `<enclosure>` with the update URL, length, and an EdDSA signature in the
//! `sparkle:` XML namespace. The feed schema is identical across the two
//! platforms (WinSparkle deliberately mirrors Sparkle's appcast), so the entry
//! model and the XML renderer live here and both adapters call them. The
//! adapters differ only in *what signs the archive* (Sparkle: an ed25519
//! `sparkle:edSignature`; WinSparkle: it trusts the Authenticode signature on
//! the installer, optionally adding its own) — captured by the optional
//! [`AppcastEntry::ed_signature`].
//!
//! The renderer is a small deterministic string builder rather than a generic
//! XML library: the appcast shape is fixed and narrow, and determinism keeps
//! the dry-run output and the unit tests stable.

use std::fmt::Write as _;

/// Release notes for an item — either an external link Sparkle fetches, or
/// inline HTML embedded in a `<description>` CDATA block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseNotes {
    /// `<sparkle:releaseNotesLink>` — Sparkle fetches and renders this URL.
    Link(String),
    /// `<description><![CDATA[…]]></description>` — inline HTML notes.
    Html(String),
}

/// A delta update from a specific prior version — rendered inside the item's
/// `<sparkle:deltas>` block alongside the full-update enclosure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaEnclosure {
    /// The `sparkle:version` (build) this delta patches *from*.
    pub delta_from: String,
    /// Public URL of the `.delta` file.
    pub url: String,
    /// Byte length of the `.delta`.
    pub length: u64,
    /// EdDSA signature over the `.delta` bytes (base64).
    pub ed_signature: Option<String>,
}

/// One appcast `<item>` — a single published update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppcastEntry {
    /// Human title (`Version 1.2.3`).
    pub title: String,
    /// `sparkle:shortVersionString` — the marketing version (`1.2.3`).
    pub short_version: String,
    /// `sparkle:version` — the monotonic build / `CFBundleVersion`. Sparkle
    /// compares updates by this, so it must increase across releases.
    pub build: String,
    /// Public URL of the full update archive (`.dmg` / `.zip` / `.exe`).
    pub enclosure_url: String,
    /// Byte length of the full update archive.
    pub length: u64,
    /// MIME type of the enclosure (`application/octet-stream` by default).
    pub mime_type: String,
    /// EdDSA signature over the archive bytes (base64). `None` when the
    /// platform relies on a different trust anchor (WinSparkle + Authenticode).
    pub ed_signature: Option<String>,
    /// `sparkle:minimumSystemVersion` (`11.0`), when constrained.
    pub min_system_version: Option<String>,
    /// `sparkle:channel` (`stable`, `beta`), when the feed is multi-channel.
    pub channel: Option<String>,
    /// Release notes (link or inline HTML).
    pub release_notes: Option<ReleaseNotes>,
    /// RFC822 `<pubDate>`, when known. Passed in (the seam has no clock).
    pub pub_date: Option<String>,
    /// Delta updates patching from prior builds.
    pub deltas: Vec<DeltaEnclosure>,
}

impl AppcastEntry {
    /// A minimal full-update entry: title/version/build + a signed enclosure.
    /// Optional fields are filled with the builder setters or left default.
    pub fn new(
        title: impl Into<String>,
        short_version: impl Into<String>,
        build: impl Into<String>,
        enclosure_url: impl Into<String>,
        length: u64,
    ) -> Self {
        Self {
            title: title.into(),
            short_version: short_version.into(),
            build: build.into(),
            enclosure_url: enclosure_url.into(),
            length,
            mime_type: "application/octet-stream".to_string(),
            ed_signature: None,
            min_system_version: None,
            channel: None,
            release_notes: None,
            pub_date: None,
            deltas: Vec::new(),
        }
    }
}

/// XML-escape text for an element body or a double-quoted attribute value.
fn xml_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// The Sparkle XML namespace URI — identical for Sparkle and WinSparkle feeds.
const SPARKLE_NS: &str = "http://www.andymatuschak.org/xml-namespaces/sparkle";

/// Render a complete appcast document for `channel_title` over `entries`.
/// Deterministic: identical inputs produce byte-identical output.
pub fn render_appcast(channel_title: &str, entries: &[AppcastEntry]) -> String {
    let mut x = String::new();
    let _ = writeln!(x, r#"<?xml version="1.0" encoding="utf-8"?>"#);
    let _ = writeln!(
        x,
        r#"<rss version="2.0" xmlns:sparkle="{SPARKLE_NS}" xmlns:dc="http://purl.org/dc/elements/1.1/">"#
    );
    let _ = writeln!(x, "  <channel>");
    let _ = writeln!(x, "    <title>{}</title>", xml_escape(channel_title));
    for e in entries {
        render_item(&mut x, e);
    }
    let _ = writeln!(x, "  </channel>");
    let _ = writeln!(x, "</rss>");
    x
}

/// Render a single `<item>`.
fn render_item(x: &mut String, e: &AppcastEntry) {
    let _ = writeln!(x, "    <item>");
    let _ = writeln!(x, "      <title>{}</title>", xml_escape(&e.title));
    let _ = writeln!(
        x,
        "      <sparkle:version>{}</sparkle:version>",
        xml_escape(&e.build)
    );
    let _ = writeln!(
        x,
        "      <sparkle:shortVersionString>{}</sparkle:shortVersionString>",
        xml_escape(&e.short_version)
    );
    if let Some(min) = &e.min_system_version {
        let _ = writeln!(
            x,
            "      <sparkle:minimumSystemVersion>{}</sparkle:minimumSystemVersion>",
            xml_escape(min)
        );
    }
    if let Some(ch) = &e.channel {
        let _ = writeln!(x, "      <sparkle:channel>{}</sparkle:channel>", xml_escape(ch));
    }
    match &e.release_notes {
        Some(ReleaseNotes::Link(url)) => {
            let _ = writeln!(
                x,
                "      <sparkle:releaseNotesLink>{}</sparkle:releaseNotesLink>",
                xml_escape(url)
            );
        }
        Some(ReleaseNotes::Html(html)) => {
            // CDATA carries HTML verbatim; guard against a literal `]]>`.
            let safe = html.replace("]]>", "]]&gt;");
            let _ = writeln!(x, "      <description><![CDATA[{safe}]]></description>");
        }
        None => {}
    }
    if let Some(date) = &e.pub_date {
        let _ = writeln!(x, "      <pubDate>{}</pubDate>", xml_escape(date));
    }
    if !e.deltas.is_empty() {
        let _ = writeln!(x, "      <sparkle:deltas>");
        for d in &e.deltas {
            render_enclosure(x, "        ", &d.url, &e.short_version, &d.delta_from, d.length,
                &e.mime_type, d.ed_signature.as_deref(), Some(&d.delta_from));
        }
        let _ = writeln!(x, "      </sparkle:deltas>");
    }
    render_enclosure(x, "      ", &e.enclosure_url, &e.short_version, &e.build, e.length,
        &e.mime_type, e.ed_signature.as_deref(), None);
    let _ = writeln!(x, "    </item>");
}

/// Render one `<enclosure>` element. `delta_from` set marks it a delta entry.
#[allow(clippy::too_many_arguments)]
fn render_enclosure(
    x: &mut String,
    indent: &str,
    url: &str,
    short_version: &str,
    build: &str,
    length: u64,
    mime_type: &str,
    ed_signature: Option<&str>,
    delta_from: Option<&str>,
) {
    let _ = write!(x, "{indent}<enclosure url=\"{}\"", xml_escape(url));
    let _ = write!(x, " sparkle:version=\"{}\"", xml_escape(build));
    let _ = write!(
        x,
        " sparkle:shortVersionString=\"{}\"",
        xml_escape(short_version)
    );
    if let Some(from) = delta_from {
        let _ = write!(x, " sparkle:deltaFrom=\"{}\"", xml_escape(from));
    }
    let _ = write!(x, " length=\"{length}\"");
    let _ = write!(x, " type=\"{}\"", xml_escape(mime_type));
    if let Some(sig) = ed_signature {
        let _ = write!(x, " sparkle:edSignature=\"{}\"", xml_escape(sig));
    }
    let _ = writeln!(x, " />");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> AppcastEntry {
        let mut e = AppcastEntry::new("Version 1.2.3", "1.2.3", "1203", "https://r/App-1.2.3.dmg", 4096);
        e.ed_signature = Some("c2lnbmF0dXJl".into());
        e.min_system_version = Some("11.0".into());
        e.channel = Some("stable".into());
        e.release_notes = Some(ReleaseNotes::Link("https://r/notes/1.2.3.html".into()));
        e
    }

    #[test]
    fn renders_well_formed_item() {
        let xml = render_appcast("Yah Desktop", &[sample()]);
        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"utf-8\"?>"));
        assert!(xml.contains(&format!("xmlns:sparkle=\"{SPARKLE_NS}\"")));
        assert!(xml.contains("<sparkle:shortVersionString>1.2.3</sparkle:shortVersionString>"));
        assert!(xml.contains("<sparkle:version>1203</sparkle:version>"));
        assert!(xml.contains("<sparkle:minimumSystemVersion>11.0</sparkle:minimumSystemVersion>"));
        assert!(xml.contains("<sparkle:channel>stable</sparkle:channel>"));
        assert!(xml.contains("<sparkle:releaseNotesLink>https://r/notes/1.2.3.html</sparkle:releaseNotesLink>"));
        assert!(xml.contains("sparkle:edSignature=\"c2lnbmF0dXJl\""));
        assert!(xml.contains("url=\"https://r/App-1.2.3.dmg\""));
        assert!(xml.contains("length=\"4096\""));
        assert!(xml.trim_end().ends_with("</rss>"));
    }

    #[test]
    fn render_is_deterministic() {
        assert_eq!(
            render_appcast("Yah Desktop", &[sample()]),
            render_appcast("Yah Desktop", &[sample()])
        );
    }

    #[test]
    fn escapes_xml_metacharacters() {
        let mut e = AppcastEntry::new("A & B <release>", "1.0", "100", "https://r/x.dmg?a=1&b=2", 1);
        e.release_notes = Some(ReleaseNotes::Html("<b>hi</b> & bye ]]> done".into()));
        let xml = render_appcast("Title & <co>", &[e]);
        assert!(xml.contains("<title>A &amp; B &lt;release&gt;</title>"));
        assert!(xml.contains("url=\"https://r/x.dmg?a=1&amp;b=2\""));
        assert!(xml.contains("<title>Title &amp; &lt;co&gt;</title>"));
        // CDATA passes HTML through but neutralizes a literal ]]> terminator.
        assert!(xml.contains("<![CDATA[<b>hi</b> & bye ]]&gt; done]]>"));
    }

    #[test]
    fn html_notes_use_cdata_description() {
        let mut e = AppcastEntry::new("v1", "1.0", "100", "https://r/x.dmg", 1);
        e.release_notes = Some(ReleaseNotes::Html("<h1>Notes</h1>".into()));
        let xml = render_appcast("T", &[e]);
        assert!(xml.contains("<description><![CDATA[<h1>Notes</h1>]]></description>"));
        assert!(!xml.contains("releaseNotesLink"));
    }

    #[test]
    fn delta_renders_in_deltas_block() {
        let mut e = AppcastEntry::new("v2", "2.0", "200", "https://r/App-2.0.dmg", 8192);
        e.deltas.push(DeltaEnclosure {
            delta_from: "100".into(),
            url: "https://r/App-2.0-from-100.delta".into(),
            length: 512,
            ed_signature: Some("ZGVsdGFzaWc=".into()),
        });
        let xml = render_appcast("T", &[e]);
        assert!(xml.contains("<sparkle:deltas>"));
        assert!(xml.contains("sparkle:deltaFrom=\"100\""));
        assert!(xml.contains("App-2.0-from-100.delta"));
        assert!(xml.contains("sparkle:edSignature=\"ZGVsdGFzaWc=\""));
        assert!(xml.contains("</sparkle:deltas>"));
    }

    #[test]
    fn omits_optional_fields_when_absent() {
        let e = AppcastEntry::new("v1", "1.0", "100", "https://r/x.dmg", 1);
        let xml = render_appcast("T", &[e]);
        assert!(!xml.contains("minimumSystemVersion"));
        assert!(!xml.contains("sparkle:channel"));
        assert!(!xml.contains("releaseNotesLink"));
        assert!(!xml.contains("description"));
        assert!(!xml.contains("edSignature"));
        assert!(!xml.contains("sparkle:deltas"));
    }
}
