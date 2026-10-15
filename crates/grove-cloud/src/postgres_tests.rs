//! Runs against isolated PostgreSQL through the same SQL-over-HTTP transport.
//! PGlite supplies CI's PostgreSQL engine; a fresh Neon project verifies hosting.
use super::*;

struct Harness {
    config: Arc<Config>,
    now: i64,
}

impl Harness {
    async fn new() -> Self {
        let mut config = if std::env::var_os("GROVE_TEST_REAL_DATABASE").is_some() {
            Config::from_environment(false).unwrap()
        } else {
            let endpoint = std::env::var("GROVE_TEST_SQL_ENDPOINT")
                .expect("start the isolated SQL fixture first");
            let endpoint = reqwest::Url::parse(&endpoint).unwrap();
            assert_eq!(endpoint.host_str(), Some("127.0.0.1"));
            Config {
                repository_id: uuid::Uuid::new_v4().to_string(),
                token: "fixture-participant-only".repeat(2),
                approver: Some("fixture-operator-only".repeat(2)),
                client: reqwest::Client::builder().no_proxy().build().unwrap(),
                endpoint,
                connection: reqwest::header::HeaderValue::from_static("fixture-unused"),
                cache: Mutex::new(cache::Cache::default()),
            }
        };
        config.repository_id = uuid::Uuid::new_v4().to_string();
        config.migrate().await.unwrap();
        Self {
            config: Arc::new(config),
            now: 10_000,
        }
    }

    fn command(&self, value: Value) -> CoordCommand {
        serde_json::from_value(value).unwrap()
    }

    async fn run(&self, value: Value, operator: bool) -> (Result<Value>, RequestMetrics) {
        let mut metrics = RequestMetrics::default();
        let result = self
            .config
            .coordinate_inner(
                Envelope {
                    repository_id: self.config.repository_id.clone(),
                    command: self.command(value),
                },
                operator,
                &mut metrics,
                Some(self.now),
            )
            .await;
        (result, metrics)
    }

    async fn snapshot(&self) -> Snapshot {
        let mut snapshot = evaluate(
            &self.config.repository_id,
            None,
            CoordCommand::Hello,
            false,
            self.now,
        )
        .unwrap()
        .0;
        for rows in snapshot.tables.values_mut() {
            rows.clear();
        }
        let data = self.config.query("SELECT table_name,data FROM grove_coordination_rows WHERE repository_id=$1 ORDER BY table_name,row_key", vec![json!(self.config.repository_id)]).await.unwrap();
        for row in data["rows"].as_array().unwrap() {
            snapshot
                .tables
                .get_mut(row[0].as_str().unwrap())
                .unwrap()
                .push(decoded(&row[1]).unwrap());
        }
        snapshot
    }

    async fn check(&mut self, value: Value, operator: bool) -> Result<Value> {
        self.now += 100;
        let before = self.snapshot().await;
        let expected = evaluate(
            &self.config.repository_id,
            Some(&before),
            self.command(value.clone()),
            operator,
            self.now,
        );
        let (actual, _) = self.run(value, operator).await;
        match (expected, &actual) {
            (Err(expected), Err(actual)) => assert_eq!(expected, *actual),
            (Ok((snapshot, mut expected)), Ok(actual)) => {
                let mut snapshot = serde_json::to_value(snapshot).unwrap();
                // UUID generation is deliberately nondeterministic. Map only
                // this operation's generated ID, then compare every stored row.
                for key in ["message_id", "claim_id"] {
                    if let (Some(old), Some(new)) = (expected[key].as_str(), actual[key].as_str()) {
                        let (old, new) = (old.to_string(), new.to_string());
                        replace_id(&mut snapshot, &old, &new);
                        replace_id(&mut expected, &old, &new);
                    }
                }
                assert_eq!(expected, *actual);
                let mut expected: Snapshot = serde_json::from_value(snapshot).unwrap();
                let mut stored = self.snapshot().await;
                for rows in expected
                    .tables
                    .values_mut()
                    .chain(stored.tables.values_mut())
                {
                    rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
                }
                assert_eq!(
                    expected.tables, stored.tables,
                    "projected operation must preserve every unrelated row"
                );
            }
            (expected, actual) => panic!(
                "engine/SQL outcome differs: {:?} / {:?}",
                expected.err(),
                actual.as_ref().err()
            ),
        }
        actual
    }

    async fn register(&mut self, id: &str, scope: &str, parent: Option<&str>) {
        self.check(
            json!({"op":"register","session_id":id,"parent_session_id":parent,
            "task":"fixture","scope":[scope],"acceptance":["preserve complete scope"],
            "observation":{"worktree_id":format!("wt-{id}"),"worktree":"/fixture",
              "branch":"main","commit":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","dirty":false,
              "verification":"client_reported"}}),
            false,
        )
        .await
        .unwrap();
    }

    async fn seed_history(&self, count: i64) {
        // Large acknowledged history is generated inside PostgreSQL. It must
        // never be transferred back for heartbeat/checkpoint/unchanged sync.
        self.config.query("WITH inserted AS (INSERT INTO grove_coordination_rows(repository_id,table_name,row_key,data) SELECT $1,'coord_messages',jsonb_build_array('history-'||n),jsonb_build_array('history-'||n,'codex:a','codex:b','old-'||n,jsonb_build_object('op','send','session_id','codex:a','to','codex:b','kind','progress','body',repeat('x',22000),'key','old-'||n)::text,1,2,3) FROM generate_series(1,$2::bigint) n ON CONFLICT DO NOTHING) INSERT INTO grove_coordination_rows(repository_id,table_name,row_key,data) SELECT $1,'coord_events',jsonb_build_array(10000+n),jsonb_build_array(10000+n,'message','codex:a','codex:b','[\"src/ab\"]',jsonb_build_object('body',repeat('x',22000))::text,1) FROM generate_series(1,$2::bigint) n ON CONFLICT DO NOTHING",vec![json!(self.config.repository_id),json!(count.to_string())]).await.unwrap();
        self.config.query("UPDATE grove_coordination SET event_seq=10000+$2::bigint,revision=revision+1 WHERE repository_id=$1",vec![json!(self.config.repository_id),json!(count.to_string())]).await.unwrap();
    }
}

fn replace_id(value: &mut Value, old: &str, new: &str) {
    match value {
        Value::String(s) if s == old => *s = new.into(),
        Value::String(s) if s.starts_with('{') || s.starts_with('[') => {
            if let Ok(mut nested) = serde_json::from_str::<Value>(s) {
                let before = nested.clone();
                replace_id(&mut nested, old, new);
                if nested != before {
                    *s = nested.to_string();
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(|v| replace_id(v, old, new)),
        Value::Object(values) => values.values_mut().for_each(|v| replace_id(v, old, new)),
        _ => {}
    }
}

#[tokio::test]
#[ignore = "requires isolated SQL HTTP PostgreSQL fixture or explicitly selected test project"]
async fn postgres_projection_atomicity_recovery_and_history_bounds() {
    let mut h = Harness::new().await;
    h.check(json!({"op":"hello"}), false).await.unwrap();
    h.register("codex:a", "src/a", None).await;
    h.register("codex:b", "src/a", Some("codex:a")).await;
    h.register("codex:c", "src/ab", None).await;
    h.register("codex:d", "app/[id]", None).await;
    h.check(json!({"op":"register","session_id":"codex:a","parent_session_id":"codex:b", "task":"cycle","scope":["."],"acceptance":["fail"],"observation":null}),false).await.unwrap_err();
    h.check(
        json!({"op":"heartbeat","session_id":"codex:a","status":"idle"}),
        false,
    )
    .await
    .unwrap();
    h.check(
        json!({"op":"checkpoint","session_id":"codex:b","cursor":999}),
        false,
    )
    .await
    .unwrap_err();
    let msg=h.check(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"port policy too","key":"one"}),false).await.unwrap();
    h.check(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"port policy too","key":"one"}),false).await.unwrap();
    h.check(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"request","body":"different","key":"one"}),false).await.unwrap_err();
    let view = h
        .check(json!({"op":"sync","session_id":"codex:b","limit":2}), false)
        .await
        .unwrap();
    assert_eq!(view["inbox"].as_array().unwrap().len(), 1);
    h.check(
        json!({"op":"checkpoint","session_id":"codex:b","cursor":view["next_cursor"]}),
        false,
    )
    .await
    .unwrap();
    let replay = h
        .check(json!({"op":"sync","session_id":"codex:b","limit":2}), false)
        .await
        .unwrap();
    assert_eq!(replay["inbox"].as_array().unwrap().len(), 1);
    let other = h
        .check(
            json!({"op":"sync","session_id":"codex:c","limit":100}),
            false,
        )
        .await
        .unwrap();
    assert!(other["inbox"].as_array().unwrap().is_empty());
    h.check(
        json!({"op":"ack_message","session_id":"codex:c","message_id":msg["message_id"]}),
        false,
    )
    .await
    .unwrap_err();
    for _ in 0..2 {
        h.check(
            json!({"op":"ack_message","session_id":"codex:b","message_id":msg["message_id"]}),
            false,
        )
        .await
        .unwrap();
    }
    for revision in 0..2 {
        h.check(json!({"op":"propose","session_id":"codex:a","decision_id":"policy","expected_revision":revision,"scope":["src/a"],"body":"include AGENTS.md","provenance":"fixture"}),false).await.unwrap();
        h.check(json!({"op":"approve","decision_id":"policy","revision":revision+1,"authority":"operator"}),false).await.unwrap_err();
        h.check(json!({"op":"approve","decision_id":"policy","revision":revision+1,"authority":"operator"}),true).await.unwrap();
        h.check(json!({"op":"ack_decision","session_id":"codex:b","decision_id":"policy","revision":revision+1}),false).await.unwrap();
    }
    h.check(
        json!({"op":"ack_decision","session_id":"codex:b","decision_id":"policy","revision":1}),
        false,
    )
    .await
    .unwrap_err();
    h.check(
        json!({"op":"ack_decision","session_id":"codex:c","decision_id":"policy","revision":2}),
        false,
    )
    .await
    .unwrap_err();
    h.check(json!({"op":"propose","session_id":"codex:d","decision_id":"route","expected_revision":0,"scope":["app/[id]/page.tsx"],"body":"literal Next.js path","provenance":"fixture"}),false).await.unwrap();
    h.check(
        json!({"op":"approve","decision_id":"route","revision":1,"authority":"operator"}),
        true,
    )
    .await
    .unwrap();
    h.check(
        json!({"op":"sync","session_id":"codex:d","limit":100}),
        false,
    )
    .await
    .unwrap();
    let claim = h
        .check(
            json!({"op":"claim","session_id":"codex:a","path":"src/a","ttl_seconds":5}),
            false,
        )
        .await
        .unwrap();
    h.check(
        json!({"op":"claim","session_id":"codex:a","path":"src/a","ttl_seconds":5}),
        false,
    )
    .await
    .unwrap();
    h.check(json!({"op":"renew_claim","session_id":"codex:b","claim_id":claim["claim_id"],"epoch":1,"ttl_seconds":5}),false).await.unwrap_err();
    h.check(json!({"op":"renew_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1,"ttl_seconds":5}),false).await.unwrap();
    h.check(
        json!({"op":"sync","session_id":"codex:b","limit":100}),
        false,
    )
    .await
    .unwrap();
    h.now += 6000;
    h.check(
        json!({"op":"sync","session_id":"codex:b","limit":100}),
        false,
    )
    .await
    .unwrap();
    h.check(json!({"op":"renew_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1,"ttl_seconds":5}),false).await.unwrap_err();
    let renewed = h
        .check(
            json!({"op":"claim","session_id":"codex:a","path":"src/a","ttl_seconds":5}),
            false,
        )
        .await
        .unwrap();
    assert_eq!(renewed["epoch"], 2);
    h.check(
        json!({"op":"release_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":1}),
        false,
    )
    .await
    .unwrap_err();
    h.check(
        json!({"op":"release_claim","session_id":"codex:a","claim_id":claim["claim_id"],"epoch":2}),
        false,
    )
    .await
    .unwrap();

    // The complete transfer test includes the policy item and commit-bound
    // review. Every step is compared to the unprojected SQLite state machine.
    let oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    h.check(json!({"op":"capture_transfer","session_id":"codex:a","transfer_id":"whole-pr","manifest":{
        "source_ref":"feature","base_ref":"base","target_ref":"HEAD","source_commit":oid,"base_commit":oid,"initial_target_commit":oid,
        "items":[{"path":"AGENTS.md","before":null,"after":{"oid":oid,"mode":"100644"},"initial_target":null}]}}),false).await.unwrap();
    h.check(
        json!({"op":"get_transfer","session_id":"codex:b","transfer_id":"whole-pr"}),
        false,
    )
    .await
    .unwrap();
    h.check(
        json!({"op":"check_transfer","session_id":"codex:a","transfer_id":"whole-pr","observation":{
        "source_commit":oid,"base_commit":oid,"target_commit":oid,"target_dirty":false,
        "target_entries":{"AGENTS.md":null}}}),
        false,
    )
    .await
    .unwrap();
    let review = json!({"op":"review_transfer","transfer_id":"whole-pr","review":{"source_commit":oid,"base_commit":oid,"target_commit":oid,"reviewer":"fixture operator",
        "items":[],"criteria":[{"index":0,"accepted":false,"evidence":"policy missing"}]}});
    h.check(review, true).await.unwrap();

    // Two overlapping sends and same-key retries exercise real CAS conflicts.
    let mut jobs = Vec::new();
    for _ in 0..8 {
        let cfg = h.config.clone();
        jobs.push(tokio::spawn(async move {
            cfg.coordinate(Envelope{repository_id:cfg.repository_id.clone(),command:serde_json::from_value(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"handoff","body":"one effect","key":"concurrent"})).unwrap()},false).await.unwrap()
        }));
    }
    let mut ids = Vec::new();
    for job in jobs {
        ids.push(job.await.unwrap()["message_id"].clone());
    }
    assert!(ids.iter().all(|id| *id == ids[0]));
    let snapshot = h.snapshot().await;
    assert_eq!(
        snapshot.tables["coord_messages"]
            .iter()
            .filter(|row| row[3] == "concurrent")
            .count(),
        1
    );

    // Different concurrent keys must all survive retries, including their
    // event/message pair, with no response for a discarded attempt.
    let mut jobs = Vec::new();
    for n in 0..4 {
        let cfg = h.config.clone();
        jobs.push(tokio::spawn(async move {
            cfg.coordinate(Envelope{repository_id:cfg.repository_id.clone(),command:serde_json::from_value(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"handoff","body":format!("distinct effect {n}"),"key":format!("parallel-{n}")})).unwrap()},false).await.unwrap()
        }));
    }
    let mut delivered = Vec::new();
    for job in jobs {
        delivered.push(job.await.unwrap()["message_id"].clone());
    }
    let snapshot = h.snapshot().await;
    for id in delivered {
        assert_eq!(
            snapshot.tables["coord_messages"]
                .iter()
                .filter(|row| row[0] == id)
                .count(),
            1
        );
        assert_eq!(
            snapshot.tables["coord_events"]
                .iter()
                .filter(|row| row[1] == "message"
                    && row[5]
                        .as_str()
                        .is_some_and(|raw| raw.contains(id.as_str().unwrap())))
                .count(),
            1
        );
    }

    // A new Config instance recovers the same durable inbox and decisions.
    let saved = h
        .check(
            json!({"op":"sync","session_id":"codex:b","limit":100}),
            false,
        )
        .await
        .unwrap();
    assert_eq!(saved["inbox"].as_array().unwrap().len(), 5);
    let source = h.snapshot().await;
    let current = h.config.as_ref();
    let restarted = Arc::new(Config {
        repository_id: current.repository_id.clone(),
        token: current.token.clone(),
        approver: current.approver.clone(),
        client: current.client.clone(),
        endpoint: current.endpoint.clone(),
        connection: current.connection.clone(),
        cache: Mutex::new(cache::Cache::default()),
    });
    let mut restarted_metrics = RequestMetrics::default();
    let restarted_view = restarted
        .coordinate_inner(
            Envelope {
                repository_id: restarted.repository_id.clone(),
                command: serde_json::from_value(
                    json!({"op":"sync","session_id":"codex:b","limit":100}),
                )
                .unwrap(),
            },
            false,
            &mut restarted_metrics,
            Some(h.now),
        )
        .await
        .unwrap();
    assert_eq!(
        saved, restarted_view,
        "recovery must not require a warm process cache"
    );
    assert_eq!(restarted_metrics.projection_cache_hits, 0);
    let destination = Harness::new().await;
    let archive = Archive {
        repository_id: destination.config.repository_id.clone(),
        revision: 500,
        snapshot: source.clone(),
    };
    let mut archive = archive;
    archive.snapshot.tables.get_mut("coord_meta").unwrap()[0][1] =
        json!(destination.config.repository_id);
    destination.config.import_snapshot(archive).await.unwrap();
    let mut restored = destination.snapshot().await;
    assert_eq!(
        restored.tables["coord_events"].len(),
        source.tables["coord_events"].len()
    );
    let mut expected = source.clone();
    expected.tables.get_mut("coord_meta").unwrap()[0][1] = json!(destination.config.repository_id);
    for rows in expected
        .tables
        .values_mut()
        .chain(restored.tables.values_mut())
    {
        rows.sort_by_key(|row| serde_json::to_string(row).unwrap());
    }
    assert_eq!(
        expected.tables, restored.tables,
        "migration must preserve all eight tables"
    );
    assert!(
        destination
            .config
            .import_snapshot(Archive {
                repository_id: destination.config.repository_id.clone(),
                revision: 500,
                snapshot: restored
            })
            .await
            .is_err()
    );

    assert!(h.config.query("UPDATE grove_coordination SET snapshot=snapshot,revision=revision+1 WHERE repository_id=$1",vec![json!(h.config.repository_id)]).await.is_err(),"even unchanged legacy snapshot writes must be fenced");

    // History-independent hot reads, including scan/page and global cursor gaps.
    let mut bytes = Vec::new();
    let counts = if std::env::var_os("GROVE_TEST_REAL_DATABASE").is_some() {
        vec![10, 100]
    } else {
        vec![10, 1000]
    };
    for count in counts {
        h.seed_history(count).await;
        let (result, heartbeat) = h
            .run(json!({"op":"heartbeat","session_id":"codex:a"}), false)
            .await;
        result.unwrap();
        assert!(heartbeat.db_response_bytes < 4096);
        let (result, sync) = h
            .run(
                json!({"op":"sync","session_id":"codex:b","after":10000+count,"limit":50}),
                false,
            )
            .await;
        let result = result.unwrap();
        assert_eq!(result["high_watermark"], 10000 + count);
        assert!(result["updates"].as_array().unwrap().is_empty());
        assert!(sync.db_response_bytes < 20_000);
        let (warm, warm_metrics) = h
            .run(
                json!({"op":"sync","session_id":"codex:b","after":10000+count,"limit":50}),
                false,
            )
            .await;
        assert_eq!(warm.unwrap(), result);
        assert_eq!(warm_metrics.projection_cache_hits, 1);
        assert!(warm_metrics.db_response_bytes < 2500);
        h.now += 100;
        h.run(json!({"op":"heartbeat","session_id":"codex:a"}), false)
            .await
            .0
            .unwrap();
        let (refreshed, refresh_metrics) = h
            .run(
                json!({"op":"sync","session_id":"codex:b","after":10000+count,"limit":50}),
                false,
            )
            .await;
        let refreshed = refreshed.unwrap();
        let peer = refreshed["peers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|peer| peer["session_id"] == "codex:a")
            .unwrap();
        assert_eq!(peer["heartbeat_at"], h.now);
        assert_eq!(
            refresh_metrics.projection_cache_hits, 1,
            "heartbeat clocks refresh without heavy-context invalidation"
        );
        let (result, scan) = h
            .run(
                json!({"op":"sync","session_id":"codex:b","after":10000,"limit":50}),
                false,
            )
            .await;
        let result = result.unwrap();
        assert!(
            result["updates"].as_array().unwrap().is_empty(),
            "src/a must not overlap src/ab"
        );
        assert!(
            scan.db_response_bytes < 300_000,
            "irrelevant historical bodies must remain in Postgres"
        );
        bytes.push(json!({"history_messages":count,"heartbeat_db_bytes":heartbeat.db_response_bytes,
            "unchanged_sync_db_bytes":sync.db_response_bytes,"warm_sync_db_bytes":warm_metrics.db_response_bytes,
            "clock_refresh_db_bytes":refresh_metrics.db_response_bytes,"header_scan_db_bytes":scan.db_response_bytes}));
    }
    h.config.query("INSERT INTO grove_coordination_rows(repository_id,table_name,row_key,data) SELECT $1,'coord_events',jsonb_build_array(30000+n),jsonb_build_array(30000+n,'session_updated','codex:c',NULL,'[\"src/ab\"]','{}',1) FROM generate_series(1,1003) n",vec![json!(h.config.repository_id)]).await.unwrap();
    h.config.query("UPDATE grove_coordination SET event_seq=31003,revision=revision+1 WHERE repository_id=$1",vec![json!(h.config.repository_id)]).await.unwrap();
    let (first, _) = h
        .run(
            json!({"op":"sync","session_id":"codex:b","after":30000,"limit":50}),
            false,
        )
        .await;
    let first = first.unwrap();
    assert_eq!(first["next_cursor"], 31000);
    assert_eq!(first["has_more"], true);
    let (last, _) = h
        .run(
            json!({"op":"sync","session_id":"codex:b","after":31000,"limit":50}),
            false,
        )
        .await;
    assert_eq!(last.unwrap()["next_cursor"], 31003);
    let (sent,_) = h.run(json!({"op":"send","session_id":"codex:a","to":"codex:b","kind":"progress","body":"after gap","key":"after-gap"}),false).await;
    sent.unwrap();
    let (view, _) = h
        .run(
            json!({"op":"sync","session_id":"codex:b","after":31003,"limit":50}),
            false,
        )
        .await;
    let view = view.unwrap();
    assert_eq!(view["updates"][0]["cursor"], 31004);
    assert_eq!(view["high_watermark"], 31004);
    if let Ok(path) = std::env::var("GROVE_TEST_ARCHIVE_PATH") {
        let mut replay = Harness::new().await;
        let mut archive: Archive = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let session = archive.snapshot.tables["coord_sessions"][0][0]
            .as_str()
            .unwrap()
            .to_string();
        let high = archive.snapshot.tables["coord_events"]
            .iter()
            .map(|row| row[0].as_i64().unwrap())
            .max()
            .unwrap();
        archive.repository_id = replay.config.repository_id.clone();
        archive.snapshot.tables.get_mut("coord_meta").unwrap()[0][1] =
            json!(replay.config.repository_id);
        replay.config.import_snapshot(archive).await.unwrap();
        let (heartbeat, hb) = replay
            .run(json!({"op":"heartbeat","session_id":session}), false)
            .await;
        heartbeat.unwrap();
        let command = json!({"op":"sync","session_id":session,"after":high,"limit":50});
        let (cold, cold_metrics) = replay.run(command.clone(), false).await;
        let cold = cold.unwrap();
        let (warm, warm_metrics) = replay.run(command.clone(), false).await;
        assert_eq!(cold, warm.unwrap());
        assert_eq!(warm_metrics.projection_cache_hits, 1);
        assert!(warm_metrics.db_response_bytes < 5000);
        replay.now += 100;
        replay
            .run(json!({"op":"heartbeat","session_id":session}), false)
            .await
            .0
            .unwrap();
        let (refresh, refresh_metrics) = replay.run(command, false).await;
        refresh.unwrap();
        assert_eq!(refresh_metrics.projection_cache_hits, 1);
        assert!(refresh_metrics.db_response_bytes < 5000);
        println!(
            "PRODUCTION_REPLAY_RECEIPT {}",
            json!({"source":"private production archive replayed in isolated test repository","model_calls":0,"heartbeat_db_bytes":hb.db_response_bytes,"cold_sync_db_bytes":cold_metrics.db_response_bytes,"warm_sync_db_bytes":warm_metrics.db_response_bytes,"clock_refresh_db_bytes":refresh_metrics.db_response_bytes})
        );
    }
    println!(
        "EGRESS_RECEIPT {}",
        json!({"bounded_history":bytes,"concurrent_same_key_effects":1,"concurrent_distinct_key_effects":4,"full_engine_equivalence":"passed","import_preserves_history_and_refuses_overwrite":true})
    );
}
