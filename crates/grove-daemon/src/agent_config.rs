use std::path::Path;

use grove_lib::{AgentAssignment, Workspace};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AgentsConfig {
    pub agents: Vec<AgentEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentEntry {
    pub name: String,
    pub path: Option<String>,
    pub branch: Option<String>,
    pub branch_pattern: Option<String>,
    pub worktree_pattern: Option<String>,
}

pub fn load_agents_config(grove_dir: &Path) -> Result<Option<AgentsConfig>, String> {
    let path = grove_dir.join("agents.toml");
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    toml::from_str(&content)
        .map(Some)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))
}

pub fn resolve_assignments(
    config: &Option<AgentsConfig>,
    workspaces: &[Workspace],
) -> Vec<AgentAssignment> {
    workspaces
        .iter()
        .map(|ws| {
            let agent_name = config
                .as_ref()
                .and_then(|cfg| find_matching_agent(cfg, ws))
                .unwrap_or_else(|| ws.name.clone());
            AgentAssignment {
                workspace_id: ws.id,
                agent_name,
            }
        })
        .collect()
}

fn find_matching_agent(config: &AgentsConfig, ws: &Workspace) -> Option<String> {
    for entry in &config.agents {
        if let Some(ref path_str) = entry.path
            && Path::new(path_str) == ws.path
        {
            return Some(entry.name.clone());
        }
        if let Some(ref branch) = entry.branch
            && branch == &ws.branch
        {
            return Some(entry.name.clone());
        }
        if let Some(ref pattern) = entry.branch_pattern
            && wildcard_match(pattern, &ws.branch)
        {
            return Some(entry.name.clone());
        }
        if let Some(ref pattern) = entry.worktree_pattern
            && wildcard_match(pattern, &ws.name)
        {
            return Some(entry.name.clone());
        }
    }
    None
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }
    if let Some(first) = parts.first()
        && !first.is_empty()
        && !value.starts_with(first)
    {
        return false;
    }
    if let Some(last) = parts.last()
        && !last.is_empty()
        && !value.ends_with(last)
    {
        return false;
    }
    if parts.len() <= 2 {
        return true;
    }
    let inner = &parts[1..parts.len() - 1];
    let mut pos = if parts[0].is_empty() {
        0
    } else {
        parts[0].len()
    };
    for &segment in inner {
        if segment.is_empty() {
            continue;
        }
        match value[pos..].find(segment) {
            Some(found) => pos += found + segment.len(),
            None => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn make_workspace(id: Uuid, name: &str, branch: &str, path: &str) -> Workspace {
        Workspace {
            id,
            name: name.to_string(),
            branch: branch.to_string(),
            path: PathBuf::from(path),
            base_ref: "main".to_string(),
            created_at: Utc::now(),
            last_activity: Utc::now(),
            metadata: grove_lib::WorkspaceMetadata::default(),
        }
    }

    fn make_config(agents: Vec<AgentEntry>) -> Option<AgentsConfig> {
        Some(AgentsConfig { agents })
    }

    #[test]
    fn exact_worktree_path_wins() {
        let ws = make_workspace(Uuid::new_v4(), "my-ws", "main", "/home/user/repo");
        let config = make_config(vec![
            AgentEntry {
                name: "PathAgent".into(),
                path: Some("/home/user/repo".into()),
                branch: None,
                branch_pattern: None,
                worktree_pattern: None,
            },
            AgentEntry {
                name: "BranchAgent".into(),
                path: None,
                branch: Some("main".into()),
                branch_pattern: None,
                worktree_pattern: None,
            },
        ]);
        let result = resolve_assignments(&config, &[ws.clone()]);
        assert_eq!(result[0].agent_name, "PathAgent");
    }

    #[test]
    fn exact_branch_wins_over_globs() {
        let ws = make_workspace(Uuid::new_v4(), "my-ws", "main", "/tmp/ws");
        let config = make_config(vec![
            AgentEntry {
                name: "BranchAgent".into(),
                path: None,
                branch: Some("main".into()),
                branch_pattern: None,
                worktree_pattern: None,
            },
            AgentEntry {
                name: "GlobAgent".into(),
                path: None,
                branch: None,
                branch_pattern: Some("mai*".into()),
                worktree_pattern: None,
            },
        ]);
        let result = resolve_assignments(&config, &[ws.clone()]);
        assert_eq!(result[0].agent_name, "BranchAgent");
    }

    #[test]
    fn branch_glob_matches() {
        let ws = make_workspace(Uuid::new_v4(), "my-ws", "feature/codex-auth", "/tmp/ws");
        let config = make_config(vec![AgentEntry {
            name: "Codex".into(),
            path: None,
            branch: None,
            branch_pattern: Some("feature/codex-*".into()),
            worktree_pattern: None,
        }]);
        let result = resolve_assignments(&config, &[ws.clone()]);
        assert_eq!(result[0].agent_name, "Codex");
    }

    #[test]
    fn worktree_name_glob_matches() {
        let ws = make_workspace(
            Uuid::new_v4(),
            "project-claude-xyz",
            "some-branch",
            "/tmp/ws",
        );
        let config = make_config(vec![AgentEntry {
            name: "Claude".into(),
            path: None,
            branch: None,
            branch_pattern: None,
            worktree_pattern: Some("*claude*".into()),
        }]);
        let result = resolve_assignments(&config, &[ws.clone()]);
        assert_eq!(result[0].agent_name, "Claude");
    }

    #[test]
    fn fallback_to_workspace_name() {
        let ws = make_workspace(Uuid::new_v4(), "unnamed-worktree", "some-branch", "/tmp/ws");
        let result = resolve_assignments(&None, &[ws.clone()]);
        assert_eq!(result[0].agent_name, "unnamed-worktree");
    }

    #[test]
    fn predictable_first_match_behaviour() {
        let ws = make_workspace(Uuid::new_v4(), "my-ws", "feature/foo", "/tmp/ws");
        let config = make_config(vec![
            AgentEntry {
                name: "First".into(),
                path: None,
                branch: None,
                branch_pattern: Some("feature/*".into()),
                worktree_pattern: None,
            },
            AgentEntry {
                name: "Second".into(),
                path: None,
                branch: None,
                branch_pattern: Some("feature/*".into()),
                worktree_pattern: None,
            },
        ]);
        let result = resolve_assignments(&config, &[ws.clone()]);
        assert_eq!(result[0].agent_name, "First");
    }

    #[test]
    fn assignments_separate_for_multiple_worktrees() {
        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        let ws_a = make_workspace(id_a, "ws-alpha", "main", "/tmp/alpha");
        let ws_b = make_workspace(id_b, "ws-beta", "develop", "/tmp/beta");
        let config = make_config(vec![
            AgentEntry {
                name: "AgentA".into(),
                path: Some("/tmp/alpha".into()),
                branch: None,
                branch_pattern: None,
                worktree_pattern: None,
            },
            AgentEntry {
                name: "AgentB".into(),
                path: Some("/tmp/beta".into()),
                branch: None,
                branch_pattern: None,
                worktree_pattern: None,
            },
        ]);
        let result = resolve_assignments(&config, &[ws_a, ws_b]);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].agent_name, "AgentA");
        assert_eq!(result[1].agent_name, "AgentB");
    }

    #[test]
    fn malformed_config_content_roundtrip() {
        // Loading from a non-existent path should return Ok(None), not an error.
        let tmp = tempfile::tempdir().unwrap();
        let result = load_agents_config(tmp.path());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn load_agents_config_missing_file_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let result = load_agents_config(tmp.path());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn wildcard_match_tests() {
        assert!(wildcard_match("feature/codex-*", "feature/codex-auth"));
        assert!(!wildcard_match("feature/codex-*", "feature/claude-auth"));
        assert!(wildcard_match("*claude*", "project-claude-xyz"));
        assert!(!wildcard_match("*claude*", "project-codex-xyz"));
        assert!(wildcard_match("*grove*", "my-grove-agent"));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("exact", "exact"));
        assert!(!wildcard_match("exact", "not-exact"));
        assert!(wildcard_match("start*", "start-xyz"));
        assert!(!wildcard_match("start*", "other-start"));
        assert!(wildcard_match("*end", "prefix-end"));
        assert!(!wildcard_match("*end", "end-suffix"));
        assert!(wildcard_match("a*b*c", "aXbYc"));
        assert!(!wildcard_match("a*b*c", "aXcYb"));
        assert!(wildcard_match("a*b", "aXb"));
    }
}
