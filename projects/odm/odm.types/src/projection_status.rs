//! Projection status reports (Phase 10-D Cache; Phase 10-F Search/Vector).
//!
//! Rebuild uses [`crate::LocalProjectionStore`] (generation + alias). Full
//! composite verify remains Phase 10-G.

use database_query::ir::{AppliedWatermark, ConsistencyIntent};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    cache_route::AppliedWatermarkState,
    topology::{ComponentRole, TopologyContract, TopologyError},
};

/// Stable format discriminator.
pub const PROJECTION_STATUS_FORMAT: &str = "iris.projection_status";

fn role_label(role: ComponentRole) -> &'static str {
    match role {
        ComponentRole::Cache => "cache",
        ComponentRole::SearchProjection => "search_projection",
        ComponentRole::VectorProjection => "vector_projection",
        ComponentRole::Replica => "replica",
        ComponentRole::Authority => "authority",
        ComponentRole::ObjectStore => "object_store",
        ComponentRole::Outbox => "outbox",
        ComponentRole::Queue => "queue",
        ComponentRole::Lock => "lock",
    }
}

fn is_reported_projection(role: ComponentRole) -> bool {
    matches!(role, ComponentRole::Cache | ComponentRole::SearchProjection | ComponentRole::VectorProjection)
}

/// Live watermark snapshot from a Cache projection (no private commands).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveWatermarkView {
    /// Whether the cache component answered.
    pub reachable: bool,
    /// Shard id when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<String>,
    /// Applied sequence when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    /// Applied wall time (unix ms) when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_unix_ms: Option<u64>,
    /// Wall-clock lag seconds vs `observed_unix_ms` when both known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_lag_secs: Option<u64>,
    /// Observation time (unix ms).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_unix_ms: Option<u64>,
}

impl LiveWatermarkView {
    /// Unreachable / disconnected cache.
    pub fn unreachable() -> Self {
        Self { reachable: false, shard: None, seq: None, applied_unix_ms: None, wall_lag_secs: None, observed_unix_ms: None }
    }

    /// Build from an applied watermark state.
    pub fn from_state(state: &AppliedWatermarkState, now_unix_ms: u64) -> Self {
        let wall_lag_secs = now_unix_ms.saturating_sub(state.applied_unix_ms).checked_div(1000);
        Self {
            reachable: true,
            shard: Some(state.watermark.shard.clone()),
            seq: Some(state.watermark.seq),
            applied_unix_ms: Some(state.applied_unix_ms),
            wall_lag_secs,
            observed_unix_ms: Some(now_unix_ms),
        }
    }

    /// Reachable but no watermark key yet.
    pub fn empty(now_unix_ms: u64) -> Self {
        Self { reachable: true, shard: None, seq: None, applied_unix_ms: None, wall_lag_secs: None, observed_unix_ms: Some(now_unix_ms) }
    }
}

/// Status for one derived projection component.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionComponentStatus {
    /// Topology component id.
    pub component: String,
    /// Role label (`cache`, ...).
    pub role: String,
    /// Datasource name from topology (not a URL).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datasource: Option<String>,
    /// Adapter family label (ops surface; not a VOS API).
    pub adapter: String,
    /// Watermark label required by a representative identity plan, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required_watermark_label: Option<String>,
    /// Optional live watermark view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveWatermarkView>,
    /// Operator notes (no secrets).
    #[serde(default)]
    pub notes: Vec<String>,
}

/// Projection status report (Cache components in 10-D).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionStatusReport {
    /// Discriminator.
    pub format: String,
    /// Document version.
    pub version: i64,
    /// Topology id.
    pub topology_id: String,
    /// Topology version.
    pub topology_version: i64,
    /// Sole authority component id.
    pub authority_id: String,
    /// Cache (and later search/vector) projection rows.
    pub projections: Vec<ProjectionComponentStatus>,
    /// Report-level notes.
    #[serde(default)]
    pub notes: Vec<String>,
}

/// Optional live Cache watermark provider (implemented by Redis adapter in CLI).
pub trait CacheWatermarkProbe {
    /// Fetch applied watermark for the default (or named) shard.
    fn probe_watermark(&self, shard: Option<&str>) -> Result<Option<AppliedWatermarkState>, String>;
}

/// Build offline projection status for Cache roles (no live I/O).
pub fn projection_status_offline(topo: &TopologyContract) -> Result<ProjectionStatusReport, TopologyError> {
    projection_status(topo, None, None)
}

/// Build projection status; optionally attach live Cache watermarks.
///
/// `live_by_component` maps topology component id -> probe. Unknown components
/// are skipped. Reports Cache / Search / Vector roles (Phase 10-F).
pub fn projection_status(
    topo: &TopologyContract,
    live_by_component: Option<&BTreeMap<String, &dyn CacheWatermarkProbe>>,
    now_unix_ms: Option<u64>,
) -> Result<ProjectionStatusReport, TopologyError> {
    topo.validate()?;
    let authority_id = topo.authority_id()?.to_string();

    // Representative identity plan to surface required watermark labels for cache.
    let eventual = topo.plan(database_query::ir::AccessKind::IdentityRead, ConsistencyIntent::Eventual, None)?;
    let required = eventual.required_watermarks;

    let mut projections = Vec::new();
    for (id, comp) in &topo.components {
        if !is_reported_projection(comp.role) {
            continue;
        }
        let mut notes = vec![
            "search/vector hits are candidates; hydrate via authority".into(),
            "rebuild uses generation isolation + alias switch (Phase 10-F)".into(),
            "projection verify (conformance) is Phase 10-G".into(),
        ];
        let required_watermark_label = required.get(id).cloned();
        if matches!(comp.role, ComponentRole::SearchProjection | ComponentRole::VectorProjection) {
            notes.push("required_watermark from search/vector plan when planned".into());
            if let Ok(plan) = topo.plan(
                if comp.role == ComponentRole::SearchProjection {
                    database_query::ir::AccessKind::Search
                }
                else {
                    database_query::ir::AccessKind::VectorNearest
                },
                ConsistencyIntent::Eventual,
                None,
            ) && let Some(label) = plan.required_watermarks.get(id)
            {
                notes.push(format!("plan_required_watermark={label}"));
            }
        }
        if required_watermark_label.is_none() && comp.role == ComponentRole::Cache {
            notes.push("no required_watermark on Eventual identity plan (cache may still exist)".into());
        }

        let live = match (comp.role, live_by_component) {
            (ComponentRole::Cache, Some(map)) => match map.get(id) {
                Some(probe) => {
                    let now = now_unix_ms.unwrap_or(0);
                    match probe.probe_watermark(None) {
                        Ok(Some(state)) => Some(LiveWatermarkView::from_state(&state, now)),
                        Ok(None) => {
                            notes.push("cache reachable; watermark key absent".into());
                            Some(LiveWatermarkView::empty(now))
                        }
                        Err(e) => {
                            notes.push(format!("cache probe failed: {e}"));
                            Some(LiveWatermarkView::unreachable())
                        }
                    }
                }
                None => {
                    notes.push("no live probe bound for this component".into());
                    None
                }
            },
            (ComponentRole::Cache, None) => {
                notes.push("offline status (pass --live to probe Cache watermarks)".into());
                None
            }
            _ => {
                notes.push("search/vector live probe not wired in status (use projection rebuild status)".into());
                None
            }
        };

        projections.push(ProjectionComponentStatus {
            component: id.clone(),
            role: role_label(comp.role).into(),
            datasource: comp.datasource.clone(),
            adapter: comp.adapter.clone(),
            required_watermark_label,
            live,
            notes,
        });
    }

    let mut notes = Vec::new();
    if projections.is_empty() {
        notes.push("topology declares no Cache/Search/Vector projection components".into());
    }
    notes.push("authority_commit_token comparable to AppliedWatermark (seq); wall_lag_secs is secondary".into());

    Ok(ProjectionStatusReport {
        format: PROJECTION_STATUS_FORMAT.into(),
        version: 1,
        topology_id: topo.id.clone(),
        topology_version: topo.topology_version,
        authority_id,
        projections,
        notes,
    })
}

/// Convenience: compare an authority head token label against a live watermark.
pub fn watermark_covers(head: &AppliedWatermark, live: &AppliedWatermark) -> bool {
    head.shard == live.shard && live.seq >= head.seq
}
