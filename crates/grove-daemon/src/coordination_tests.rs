use super::*;

struct Fixture {
    conn: Connection,
    repo: String,
    now: i64,
}
impl Fixture {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        let repo = repository_id(&conn).unwrap();
        Self {
            conn,
            repo,
            now: 1_000_000,
        }
    }
    fn call(&mut self, value: Value) -> Value {
        self.try_call(value, false).unwrap()
    }
    fn admin(&mut self, value: Value) -> Value {
        self.try_call(value, true).unwrap()
    }
    fn try_call(&mut self, value: Value, admin: bool) -> Result<Value> {
        execute(
            &mut self.conn,
            Some(&self.repo),
            serde_json::from_value(value).unwrap(),
            self.now,
            admin,
        )
    }
    fn register(&mut self, id: &str, branch: &str, scope: &str) -> Value {
        self.call(json!({"op":"register","session_id":id,"task":"port entire requested PR","scope":[scope],"acceptance":["runtime and repository policy both incorporated"],
            "observation":{"worktree_id":format!("wt-{id}"),"worktree":format!("/disposable/{id}"),"branch":branch,"commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dirty":false,"pr_url":null}}))
    }
    fn sync(&mut self, id: &str) -> Value {
        self.call(json!({"op":"sync","session_id":id}))
    }
    fn send(&mut self, key: &str) -> Value {
        self.call(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"port the complete PR including AGENTS.md","key":key}))
    }
    fn pair(&mut self) {
        self.register("codex:a", "runtime", ".");
        self.register("codex:b", "policy", ".");
    }
    fn propose(&mut self, revision: i64) -> Value {
        self.call(json!({"op":"propose","session_id":"codex:a","decision_id":"testing","expected_revision":revision,"scope":["."],"body":format!("run focused checks revision {}",revision+1),"provenance":"operator requirement recorded in test"}))
    }
    fn approve(&mut self, revision: i64) {
        self.admin(json!({"op":"approve","decision_id":"testing","revision":revision,"authority":"test operator"}));
    }
}

#[test]
fn repository_identity_is_persistent_and_repositories_are_isolated() {
    let mut a = Fixture::new();
    let b = Fixture::new();
    assert_ne!(a.repo, b.repo);
    let id = a.repo.clone();
    let reply = execute(&mut a.conn, Some(&id), CoordCommand::Hello, a.now, false).unwrap();
    assert_eq!(reply["repository_id"], id);
    assert!(
        execute(
            &mut a.conn,
            Some(&b.repo),
            CoordCommand::Hello,
            a.now,
            false
        )
        .is_err()
    );
    assert!(
        execute(
            &mut a.conn,
            None,
            CoordCommand::Sync {
                session_id: "codex:a".into(),
                after: None,
                limit: 50
            },
            a.now,
            false
        )
        .is_err()
    );
}

#[test]
fn identities_are_provider_qualified_and_parent_cycles_fail() {
    let mut f = Fixture::new();
    f.pair();
    f.register("opencode:a", "runtime", ".");
    assert_eq!(f.sync("codex:a")["peers"].as_array().unwrap().len(), 3);
    let mut child = serde_json::to_value(session(&f.conn, "codex:b").unwrap()).unwrap();
    // Build the registration contract explicitly; never copy Session's server fields.
    child = json!({"op":"register","session_id":"codex:b","task":child["task"],"scope":["."],"acceptance":child["acceptance"],"observation":child["observation"],"parent_session_id":"codex:a"});
    f.call(child);
    let a = session(&f.conn, "codex:a").unwrap();
    assert!(f.try_call(json!({"op":"register","session_id":"codex:a","task":a.task,"scope":a.scope,"acceptance":a.acceptance,"observation":a.observation,"parent_session_id":"codex:b"}),false).is_err());
}

#[test]
fn proposals_never_replace_approved_instructions_and_revision_ack_is_exact() {
    let mut f = Fixture::new();
    f.pair();
    f.propose(0);
    assert!(
        f.sync("codex:b")["current_decisions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(f.try_call(json!({"op":"approve","decision_id":"testing","revision":1,"authority":"arbitrary agent"}),false).is_err());
    f.approve(1);
    f.call(
        json!({"op":"ack_decision","session_id":"codex:b","decision_id":"testing","revision":1}),
    );
    f.propose(1);
    let view = f.sync("codex:b");
    assert_eq!(view["current_decisions"][0]["revision"], 1);
    assert_eq!(view["proposals"][0]["revision"], 2);
    f.approve(2);
    let view = f.sync("codex:b");
    assert_eq!(view["current_decisions"].as_array().unwrap().len(), 1);
    assert_eq!(view["current_decisions"][0]["revision"], 2);
    assert_eq!(view["current_decisions"][0]["acknowledged"], false);
    assert!(f.try_call(json!({"op":"ack_decision","session_id":"codex:b","decision_id":"testing","revision":1}),false).is_err());
    f.call(
        json!({"op":"ack_decision","session_id":"codex:b","decision_id":"testing","revision":2}),
    );
    assert_eq!(
        f.sync("codex:b")["current_decisions"][0]["acknowledged"],
        true
    );
}

#[test]
fn concurrent_revision_preconditions_prevent_lost_updates() {
    let mut f = Fixture::new();
    f.pair();
    f.propose(0);
    assert!(f.try_call(json!({"op":"propose","session_id":"codex:b","decision_id":"testing","expected_revision":0,"scope":["."],"body":"racing proposal","provenance":"test"}),false).is_err());
    assert_eq!(f.sync("codex:a")["proposals"][0]["revision"], 1);
}

#[test]
fn scopes_are_component_based_and_irrelevant_decisions_are_hidden() {
    assert!(!scopes_overlap("src/a", "src/ab"));
    assert!(scopes_overlap("src/a", "src/a/file.rs"));
    for p in [
        "../escape",
        "/absolute",
        "a//b",
        "a/../b",
        "a/*",
        "C:drive",
        "a\\b",
    ] {
        assert!(!valid_scope(p));
    }
    let mut f = Fixture::new();
    f.register("codex:a", "a", "src/a");
    f.register("codex:b", "b", "src/ab");
    f.call(json!({"op":"propose","session_id":"codex:a","decision_id":"scope","expected_revision":0,"scope":["src/a"],"body":"scope decision","provenance":"test"}));
    f.admin(json!({"op":"approve","decision_id":"scope","revision":1,"authority":"test operator"}));
    assert!(
        f.sync("codex:b")["current_decisions"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        f.sync("codex:b")["assignment_overlaps"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn durable_messages_retry_without_duplicate_effects_and_reject_changed_keys() {
    let mut f = Fixture::new();
    f.pair();
    let sent = f.send("port-1");
    let retry = f.send("port-1");
    assert_eq!(sent["message_id"], retry["message_id"]);
    assert_eq!(retry["deduplicated"], true);
    assert!(f.try_call(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"different content","key":"port-1"}),false).is_err());
    assert_eq!(f.sync("codex:b")["inbox"].as_array().unwrap().len(), 1);
    assert!(
        f.try_call(
            json!({"op":"ack_message","session_id":"codex:a","message_id":sent["message_id"]}),
            false
        )
        .is_err()
    );
    f.call(json!({"op":"ack_message","session_id":"codex:b","message_id":sent["message_id"]}));
    f.call(json!({"op":"ack_message","session_id":"codex:b","message_id":sent["message_id"]}));
    assert!(f.sync("codex:b")["inbox"].as_array().unwrap().is_empty());
    let count: i64 = f
        .conn
        .query_row(
            "SELECT COUNT(*) FROM coord_events WHERE kind='message_acknowledged'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn service_delivery_and_model_acknowledgement_have_separate_timestamps() {
    let mut f = Fixture::new();
    f.pair();
    let sent = f.send("latency");
    f.now += 5000;
    let inbox = f.sync("codex:b");
    assert_eq!(
        inbox["inbox"][0]["service_read_at"].as_i64().unwrap()
            - inbox["inbox"][0]["enqueued_at"].as_i64().unwrap(),
        5000
    );
    f.now += 20_000;
    let ack =
        f.call(json!({"op":"ack_message","session_id":"codex:b","message_id":sent["message_id"]}));
    assert_eq!(
        ack["acknowledged_at"].as_i64().unwrap() - ack["service_read_at"].as_i64().unwrap(),
        20_000
    );
}

#[test]
fn checkpoints_cannot_skip_unserved_updates_and_do_not_drop_pending_inbox() {
    let mut f = Fixture::new();
    f.pair();
    f.send("pending");
    assert!(
        f.try_call(
            json!({"op":"checkpoint","session_id":"codex:b","cursor":999}),
            false
        )
        .is_err()
    );
    let page = f.call(json!({"op":"sync","session_id":"codex:b","limit":1}));
    assert_eq!(page["has_more"], true);
    f.call(json!({"op":"checkpoint","session_id":"codex:b","cursor":page["next_cursor"]}));
    let view = f.sync("codex:b");
    f.call(json!({"op":"checkpoint","session_id":"codex:b","cursor":view["next_cursor"]}));
    let recovered = f.sync("codex:b");
    assert!(recovered["updates"].as_array().unwrap().is_empty());
    assert_eq!(recovered["inbox"].as_array().unwrap().len(), 1);
    assert!(
        f.try_call(
            json!({"op":"checkpoint","session_id":"codex:b","cursor":0}),
            false
        )
        .is_err()
    );
}

#[test]
fn targeted_message_events_do_not_leak_to_unaddressed_sessions() {
    let mut f = Fixture::new();
    f.pair();
    f.register("codex:c", "c", ".");
    f.send("private");
    let view = f.sync("codex:c");
    assert!(view["inbox"].as_array().unwrap().is_empty());
    assert!(
        !view["updates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["kind"] == "message")
    );
}

#[test]
fn database_restart_recovers_context_pending_inbox_and_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grove.db");
    let mut f = Fixture::new();
    f.conn = Connection::open(&path).unwrap();
    migrate(&f.conn).unwrap();
    f.repo = repository_id(&f.conn).unwrap();
    f.pair();
    f.propose(0);
    f.approve(1);
    let message = f.send("restart");
    let view = f.sync("codex:b");
    f.call(json!({"op":"checkpoint","session_id":"codex:b","cursor":view["next_cursor"]}));
    let repo = f.repo.clone();
    drop(f.conn);
    let conn = Connection::open(&path).unwrap();
    migrate(&conn).unwrap();
    let mut recovered = Fixture {
        conn,
        repo,
        now: 1_010_000,
    };
    recovered.register("codex:b", "policy", ".");
    let view = recovered.sync("codex:b");
    assert_eq!(view["checkpoint"], view["next_cursor"]);
    assert_eq!(view["inbox"][0]["message_id"], message["message_id"]);
    assert_eq!(view["current_decisions"][0]["revision"], 1);
    assert_eq!(recovered.send("restart")["deduplicated"], true);
}

#[test]
fn stale_claims_are_visible_and_recovery_fences_old_owners() {
    let mut f = Fixture::new();
    f.pair();
    let claim = f.call(json!({"op":"claim","session_id":"codex:a","path":"src","ttl_seconds":5}));
    f.call(json!({"op":"claim","session_id":"codex:b","path":"src/runtime.ts","ttl_seconds":5}));
    let view = f.sync("codex:a");
    assert!(
        view["claims"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| !c["overlaps"].as_array().unwrap().is_empty())
    );
    f.now += DEFAULT_STALE_MS + 1;
    let stale = f.sync("codex:a");
    assert!(
        stale["claims"]
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["expired"] == true && c["stale_owner"] == true)
    );
    assert!(f.try_call(json!({"op":"renew_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1,"ttl_seconds":5}),false).is_err());
    f.call(json!({"op":"heartbeat","session_id":"codex:a","status":"active"}));
    let recovered =
        f.call(json!({"op":"claim","session_id":"codex:a","path":"src","ttl_seconds":90}));
    assert_eq!(recovered["claim_id"], claim["claim_id"]);
    assert_eq!(recovered["epoch"], 2);
    assert!(f.try_call(json!({"op":"release_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1}),false).is_err());
    assert!(f.try_call(json!({"op":"release_claim","session_id":"codex:b","claim_id":claim["claim_id"],"epoch":2}),false).is_err());
    f.call(
        json!({"op":"release_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":2}),
    );
}

fn manifest() -> TransferManifest {
    let before = Some(TreeEntry {
        mode: "100644".into(),
        oid: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".into(),
    });
    let after = Some(TreeEntry {
        mode: "100644".into(),
        oid: "cccccccccccccccccccccccccccccccccccccccc".into(),
    });
    TransferManifest {
        source_ref: "source".into(),
        base_ref: "base".into(),
        target_ref: "target".into(),
        source_commit: "1111111111111111111111111111111111111111".into(),
        base_commit: "0000000000000000000000000000000000000000".into(),
        initial_target_commit: "2222222222222222222222222222222222222222".into(),
        pr_url: Some("https://example.test/pr/1".into()),
        items: ["src/runtime.ts", "AGENTS.md"]
            .iter()
            .map(|p| TransferItem {
                path: p.to_string(),
                before: before.clone(),
                after: after.clone(),
                initial_target: before.clone(),
            })
            .collect(),
    }
}
fn capture(f: &mut Fixture) {
    f.call(json!({"op":"capture_transfer","session_id":"codex:a","transfer_id":"whole-pr","manifest":manifest()}));
}
fn observation_complete(policy: bool) -> TransferObservation {
    let m = manifest();
    let entries = m
        .items
        .iter()
        .map(|i| {
            (
                i.path.clone(),
                if i.path == "AGENTS.md" && !policy {
                    i.before.clone()
                } else {
                    i.after.clone()
                },
            )
        })
        .collect();
    TransferObservation {
        source_commit: m.source_commit,
        base_commit: m.base_commit,
        target_commit: "3333333333333333333333333333333333333333".into(),
        target_entries: entries,
        target_dirty: false,
    }
}
fn checked(f: &mut Fixture, obs: TransferObservation) -> Value {
    f.call(json!({"op":"check_transfer","session_id":"codex:a","transfer_id":"whole-pr","observation":obs}))
}
fn review() -> TransferReview {
    let obs = observation_complete(true);
    TransferReview {
        source_commit: obs.source_commit,
        base_commit: obs.base_commit,
        target_commit: obs.target_commit,
        reviewer: "test operator".into(),
        items: vec![],
        criteria: vec![CriterionReview {
            index: 0,
            accepted: true,
            evidence: "manually compared runtime and AGENTS instructions at the recorded commits"
                .into(),
        }],
    }
}

#[test]
fn incomplete_runtime_only_transfer_exposes_omitted_policy_and_requires_review() {
    let mut f = Fixture::new();
    f.pair();
    capture(&mut f);
    let incomplete = checked(&mut f, observation_complete(false));
    assert_eq!(incomplete["unresolved"], json!(["AGENTS.md"]));
    assert_eq!(incomplete["complete_at_observed_commits"], false);
    let complete = checked(&mut f, observation_complete(true));
    assert!(complete["unresolved"].as_array().unwrap().is_empty());
    assert_eq!(complete["complete_at_observed_commits"], false);
    let reviewed =
        f.admin(json!({"op":"review_transfer","transfer_id":"whole-pr","review":review()}));
    assert_eq!(reviewed["complete_at_observed_commits"], true);
}

#[test]
fn changed_source_target_base_or_assignment_invalidates_completion_evidence() {
    for change in ["source", "target", "base", "assignment", "dirty"] {
        let mut f = Fixture::new();
        f.pair();
        capture(&mut f);
        checked(&mut f, observation_complete(true));
        f.admin(json!({"op":"review_transfer","transfer_id":"whole-pr","review":review()}));
        let mut obs = observation_complete(true);
        match change {
            "source" => obs.source_commit = "4444444444444444444444444444444444444444".into(),
            "target" => obs.target_commit = "4444444444444444444444444444444444444444".into(),
            "base" => obs.base_commit = "4444444444444444444444444444444444444444".into(),
            "dirty" => obs.target_dirty = true,
            _ => {
                f.call(json!({"op":"register","session_id":"codex:a","task":"different assignment","scope":["."],"acceptance":["different criteria"],"observation":session(&f.conn,"codex:a").unwrap().observation}));
            }
        }
        assert_eq!(
            checked(&mut f, obs)["complete_at_observed_commits"],
            false,
            "{change}"
        );
    }
}

#[test]
fn transfer_checks_cannot_omit_paths_and_semantic_exclusions_need_operator_evidence() {
    let mut f = Fixture::new();
    f.pair();
    capture(&mut f);
    let mut obs = observation_complete(true);
    obs.target_entries.remove("AGENTS.md");
    assert!(f.try_call(json!({"op":"check_transfer","session_id":"codex:a","transfer_id":"whole-pr","observation":obs}),false).is_err());
    checked(&mut f, observation_complete(false));
    let mut r = review();
    r.items.push(ItemReview{path:"AGENTS.md".into(),disposition:Disposition::DeliberatelyExcluded,evidence:"operator approved keeping stricter target policy; compared both policies at these commits".into()});
    assert!(
        f.try_call(
            json!({"op":"review_transfer","transfer_id":"whole-pr","review":r}),
            false
        )
        .is_err()
    );
    assert_eq!(
        f.admin(json!({"op":"review_transfer","transfer_id":"whole-pr","review":r}))["complete_at_observed_commits"],
        true
    );
}

#[test]
fn known_peer_commit_change_invalidates_old_transfer_without_another_git_check() {
    let mut f = Fixture::new();
    f.pair();
    capture(&mut f);
    checked(&mut f, observation_complete(true));
    f.admin(json!({"op":"review_transfer","transfer_id":"whole-pr","review":review()}));
    f.now += 1000;
    f.register("codex:c", "source", ".");
    let status =
        f.call(json!({"op":"get_transfer","session_id":"codex:b","transfer_id":"whole-pr"}));
    assert_eq!(status["status"]["complete_at_observed_commits"], false);
    assert_eq!(
        status["status"]["known_ref_changes"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn codex_adapter_exposes_only_registered_metadata_and_never_exact_file_attribution() {
    use crate::provider::{EventCapability, ExactFileActivityCapability, ProviderAdapter};
    let mut f = Fixture::new();
    f.pair();
    let mut s = session(&f.conn, "codex:a").unwrap();
    assert!(crate::codex::normalize(&s).is_none());
    s.observation.verification = "local_git".into();
    let update = crate::codex::normalize(&s).unwrap();
    assert_eq!(update.native_session_id, "a");
    let adapter = crate::codex::CodexAdapter::new("unused".into());
    assert_eq!(
        adapter.capabilities().exact_file_activity,
        ExactFileActivityCapability::Unsupported
    );
    assert_eq!(adapter.capabilities().events, EventCapability::Unsupported);
    assert!(crate::codex::credential_matches(
        Some("operator-test"),
        Some("operator-test")
    ));
    assert!(!crate::codex::credential_matches(Some(""), Some("")));
    assert!(!crate::codex::credential_matches(None, None));
}
