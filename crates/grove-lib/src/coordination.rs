//! Transport-neutral coordination contracts. Messages and proposed decisions
//! are context, never authority to run commands or change repository policy.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const DEFAULT_STALE_MS: i64 = 120_000;
pub const MAX_PAGE: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GitObservation {
    pub worktree_id: String,
    pub worktree: String,
    pub branch: String,
    pub commit: String,
    pub dirty: bool,
    pub pr_url: Option<String>,
    #[serde(default)]
    pub base_commit: Option<String>,
    #[serde(default)]
    pub changed_paths: Vec<String>,
    #[serde(default)]
    pub changes_truncated: bool,
    /// Set by the service, never accepted as proof from a client.
    #[serde(default)]
    pub verification: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub provider: String,
    pub native_session_id: String,
    pub parent_session_id: Option<String>,
    pub task: String,
    pub scope: Vec<String>,
    pub acceptance: Vec<String>,
    pub observation: GitObservation,
    pub status: String,
    pub registered_at: i64,
    pub heartbeat_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TreeEntry {
    pub mode: String,
    pub oid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferItem {
    pub path: String,
    pub before: Option<TreeEntry>,
    pub after: Option<TreeEntry>,
    pub initial_target: Option<TreeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferManifest {
    pub source_ref: String,
    pub base_ref: String,
    pub target_ref: String,
    pub source_commit: String,
    pub base_commit: String,
    pub initial_target_commit: String,
    pub pr_url: Option<String>,
    pub items: Vec<TransferItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferObservation {
    pub source_commit: String,
    pub base_commit: String,
    pub target_commit: String,
    /// Every manifest path, including absent/deleted paths, must be present.
    pub target_entries: std::collections::BTreeMap<String, Option<TreeEntry>>,
    pub target_dirty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Incorporated,
    AlreadyPresent,
    Superseded,
    DeliberatelyExcluded,
    Unresolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ItemReview {
    pub path: String,
    pub disposition: Disposition,
    pub evidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CriterionReview {
    pub index: usize,
    pub accepted: bool,
    pub evidence: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TransferReview {
    pub source_commit: String,
    pub base_commit: String,
    pub target_commit: String,
    pub reviewer: String,
    pub items: Vec<ItemReview>,
    pub criteria: Vec<CriterionReview>,
}

/// Commands have one owner: Grove's daemon actor and database. Operator-only
/// approval/review operations are excluded from the MCP tool surface.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum CoordCommand {
    Hello,
    Register {
        session_id: String,
        task: String,
        scope: Vec<String>,
        acceptance: Vec<String>,
        #[serde(default)]
        parent_session_id: Option<String>,
        #[serde(default)]
        observation: Option<GitObservation>,
    },
    Heartbeat {
        session_id: String,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        observation: Option<GitObservation>,
    },
    Sync {
        session_id: String,
        #[serde(default)]
        after: Option<i64>,
        #[serde(default = "default_page")]
        limit: usize,
    },
    Checkpoint {
        session_id: String,
        cursor: i64,
    },
    Send {
        session_id: String,
        to: String,
        kind: String,
        body: String,
        /// Stable key chosen before sending; re-use it on network retries.
        key: String,
    },
    AckMessage {
        session_id: String,
        message_id: String,
    },
    Propose {
        session_id: String,
        decision_id: String,
        expected_revision: i64,
        scope: Vec<String>,
        body: String,
        provenance: String,
    },
    Approve {
        decision_id: String,
        revision: i64,
        authority: String,
    },
    AckDecision {
        session_id: String,
        decision_id: String,
        revision: i64,
    },
    Claim {
        session_id: String,
        path: String,
        ttl_seconds: i64,
    },
    RenewClaim {
        session_id: String,
        claim_id: String,
        epoch: i64,
        ttl_seconds: i64,
    },
    ReleaseClaim {
        session_id: String,
        claim_id: String,
        epoch: i64,
    },
    CaptureTransfer {
        session_id: String,
        transfer_id: String,
        manifest: TransferManifest,
    },
    GetTransfer {
        session_id: String,
        transfer_id: String,
    },
    CheckTransfer {
        session_id: String,
        transfer_id: String,
        observation: TransferObservation,
    },
    ReviewTransfer {
        transfer_id: String,
        review: TransferReview,
    },
}

fn default_page() -> usize {
    50
}

impl CoordCommand {
    pub fn operator_only(&self) -> bool {
        matches!(self, Self::Approve { .. } | Self::ReviewTransfer { .. })
    }

    pub fn session_id(&self) -> Option<&str> {
        match self {
            Self::Hello | Self::Approve { .. } | Self::ReviewTransfer { .. } => None,
            Self::Register { session_id, .. }
            | Self::Heartbeat { session_id, .. }
            | Self::Sync { session_id, .. }
            | Self::Checkpoint { session_id, .. }
            | Self::Send { session_id, .. }
            | Self::AckMessage { session_id, .. }
            | Self::Propose { session_id, .. }
            | Self::AckDecision { session_id, .. }
            | Self::Claim { session_id, .. }
            | Self::RenewClaim { session_id, .. }
            | Self::ReleaseClaim { session_id, .. }
            | Self::CaptureTransfer { session_id, .. }
            | Self::GetTransfer { session_id, .. }
            | Self::CheckTransfer { session_id, .. } => Some(session_id),
        }
    }
}

/// Literal repository-relative files/directories; '.' covers the repository.
/// Prefix matching is component-based, so 'src/a' never matches 'src/ab'.
pub fn scopes_overlap(a: &str, b: &str) -> bool {
    a == "."
        || b == "."
        || a == b
        || a.strip_prefix(b).is_some_and(|s| s.starts_with('/'))
        || b.strip_prefix(a).is_some_and(|s| s.starts_with('/'))
}

pub fn relevant(a: &[String], b: &[String]) -> bool {
    a.iter().any(|x| b.iter().any(|y| scopes_overlap(x, y)))
}

pub fn valid_scope(path: &str) -> bool {
    path == "."
        || (!path.is_empty()
            && path.len() <= 4096
            && !path.starts_with('/')
            && !path.ends_with('/')
            // Brackets in Next.js route names are literal, never glob syntax.
            && !path.contains(['\\', '\0', '*', '?', '\n', '\r', ':'])
            && path
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."))
}
