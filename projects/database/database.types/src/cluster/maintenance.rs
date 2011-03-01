//! Maintenance counters shared by storage and execution.

/// Outcome of one physical expiry reclamation pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Records inspected in this pass.
    pub examined: usize,
    /// Records physically removed in this pass.
    pub reclaimed: usize,
}
