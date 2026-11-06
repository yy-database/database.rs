//! Project loading and schema I/O (shared by CLI + embedders).
//!
//! Implementation lives in `odm-types` so host/adapters never depend on this
//! facade crate and no cycle forms.

use std::path::Path;

pub use odm_types::{
    DatasourceConfig, DatasourceKind, GenerateConfig, IrisProject, RUNTIME_PROJECT_FILE, TruthMode, discover_project_config, expand_endpoint,
    load_project, load_project_optional, load_project_required, materialize_runtime_project, resolve_path, resolve_project_root, write_file,
};

/// Read merged VOS schema text from project config (`schema` points at data files; glob ok).
pub fn read_schema(project_dir: &Path, project: &IrisProject) -> Result<String, String> {
    odm_types::read_schema(project_dir, &project.schema)
}
