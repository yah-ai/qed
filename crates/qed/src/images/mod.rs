//! Container image catalog + layering compiler.
//!
//! - [`catalog`] (R381-T1): the bundled manifest of yah-managed images plus
//!   per-camp overrides.
//! - [`compile`] (R381-T3): turns a catalog entry's TOML layering shorthand
//!   into a Dockerfile string (or returns a sibling Dockerfile verbatim).
//!
//! Build dispatch (`qed::build-image` step kind) lives in
//! [`crate::runner`] — see R381-T2; real execution lands in T4/T5.

pub mod catalog;
pub mod compile;

pub use catalog::{CatalogEntry, CatalogError, CatalogManifest, ProduceTarget};
pub use compile::{
    catalog_image_ref, compile_entry, compile_with_dockerfile_dir, CompileError, MAX_EXTENDS_DEPTH,
};
