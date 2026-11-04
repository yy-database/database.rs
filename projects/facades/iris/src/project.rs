//! Project loading and schema I/O (shared by CLI + embedders).
//!
//! Implementation lives in `iris-types` so host/adapters never depend on this
//! facade crate (avoids `iris` ↔ `iris-host` cycles).

use std::path::Path;

pub use iris_types::{
    IrisProject, RUNTIME_PROJECT_FILE, discover_project_config, expand_endpoint, load_project, load_project_optional, load_project_required,
    materialize_runtime_project, resolve_project_root, write_file,
};

/// Read merged VOS schema text from project config (`schema` field points at data files; glob ok).
pub fn read_schema(project_dir: &Path, project: &IrisProject) -> Result<String, String> {
    iris_types::read_schema(project_dir, &project.schema)
}
