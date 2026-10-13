#!/usr/bin/env python3
"""Black-box acceptance through real Grove processes, Git, HTTP and MCP.

All mutations are confined to disposable repositories. No paid model calls.
Run after `cargo build -p grove`; uses only the Python standard library.
"""
import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import secrets
import select
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


class Harness:
    def __init__(self, binary, root):
        self.binary = str(binary)
        self.root = root
        self.env = os.environ.copy()
        for key in ("GROVE_COORD_URL", "GROVE_COORD_TOKEN", "GROVE_REPOSITORY_ID", "GROVE_COORD_APPROVER_TOKEN"):
            self.env.pop(key, None)
        # Credentials exist only for this disposable, loopback fixture.
        self.env["GROVE_COORD_APPROVER_TOKEN"] = secrets.token_urlsafe(32)
        self.env["GROVE_COORD_TOKEN"] = secrets.token_urlsafe(32)
        self.children = []
        self.logs = []
        self.assertions = 0

    def check(self, condition, message):
        assert condition, message
        self.assertions += 1

    def process(self, cwd, args, env=None, ok=True):
        result = subprocess.run(args, cwd=cwd, env=env or self.env, capture_output=True, text=True, timeout=30)
        if ok and result.returncode:
            raise AssertionError(f"command failed: {args[:3]}\n{result.stderr}")
        if not ok:
            self.check(result.returncode != 0, "request should have failed")
        return result

    def git(self, cwd, *args):
        return self.process(cwd, ["git", *args]).stdout.strip()

    def grove(self, cwd, *args, env=None, ok=True):
        if env is None:
            env = self.env.copy()
            env.pop("GROVE_COORD_TOKEN", None)
        result = self.process(cwd, [self.binary, "--json", *args], env, ok)
        return json.loads(result.stdout) if ok else result

    def launch(self, cwd, *args):
        log = tempfile.TemporaryFile(mode="w+")
        self.logs.append(log)
        child = subprocess.Popen([self.binary, *args], cwd=cwd, env=self.env, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        self.children.append(child)
        return child

    def daemon(self, cwd):
        child = self.launch(cwd, "daemon", "start")
        sock = cwd / ".git" / "grove" / "daemon.sock"
        for _ in range(150):
            if sock.exists():
                return child
            if child.poll() is not None:
                raise AssertionError("daemon did not start")
            time.sleep(.02)
        raise AssertionError("daemon readiness timeout")

    def cleanup(self):
        for child in reversed(self.children):
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
        for log in self.logs:
            log.close()


def mcp_roundtrip(h, cwd, env):
    process = subprocess.Popen([h.binary, "mcp", "--repo", str(cwd)], env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
    counter = 0

    def request(method, params):
        nonlocal counter
        counter += 1
        process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": counter, "method": method, "params": params}) + "\n")
        process.stdin.flush()
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            ready, _, _ = select.select([process.stdout], [], [], max(0, deadline-time.monotonic()))
            if not ready:
                break
            line = process.stdout.readline()
            if not line:
                break
            response = json.loads(line)
            if response.get("id") == counter:
                assert "error" not in response, response
                return response["result"]
        raise AssertionError("MCP response timeout")

    try:
        initialized = request("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "grove-acceptance", "version": "1"}})
        process.stdin.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        process.stdin.flush()
        tools = request("tools/list", {})["tools"]
        names = {t["name"] for t in tools}
        h.check("grove_register" in names and "grove_sync" in names, "MCP exposes registration and recovery")
        h.check("grove_approve" not in names and "grove_review_transfer" not in names, "operator approvals are absent from MCP")
        result = request("tools/call", {"name": "grove_register", "arguments": {"session_id": "codex:mcp-participant", "task": "MCP interoperability", "scope": ["."], "acceptance": ["recover peer context without file edits"]}})
        h.check(not result.get("isError"), "MCP registration succeeds")
        view = request("tools/call", {"name": "grove_sync", "arguments": {"session_id": "codex:mcp-participant"}})
        data = view.get("structuredContent") or json.loads(view["content"][0]["text"])
        h.check(len(data["peers"]) >= 3, "MCP participant discovers independently registered peers")
        return {"tool_count": len(tools), "negotiated_protocol": initialized["protocolVersion"]}
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)


def run(binary):
    with tempfile.TemporaryDirectory(prefix="grove-acceptance-") as directory:
        root = Path(directory)
        repo = root / "repo"
        repo.mkdir()
        h = Harness(binary, root)
        try:
            h.git(repo, "init", "-b", "main")
            h.git(repo, "config", "user.email", "grove-acceptance@example.test")
            h.git(repo, "config", "user.name", "Grove acceptance fixture")
            (repo / "runtime.js").write_text("export const runtime = 1;\n")
            (repo / "AGENTS.md").write_text("Fixture policy: use narrow checks.\n")
            (repo / "config.json").write_text('{"version":1}\n')
            (repo / "guide.md").write_text("fixture guide v1\n")
            h.git(repo, "add", ".")
            h.git(repo, "commit", "-m", "fixture base")
            base = h.git(repo, "rev-parse", "HEAD")
            source, target = root / "source", root / "target"
            h.git(repo, "worktree", "add", "-b", "source", str(source))
            h.git(repo, "worktree", "add", "-b", "target", str(target))
            for path, body in {"runtime.js": "export const runtime = 2;\n", "AGENTS.md": "Fixture policy v2: include policy in branch transfers.\n", "config.json": '{"version":2}\n', "guide.md": "fixture guide v2\n", "runtime.test.js": "// fixture acceptance test v2\n"}.items():
                (source / path).write_text(body)
            h.git(source, "add", ".")
            h.git(source, "commit", "-m", "runtime, policy, configuration, tests and documentation")
            daemon = h.daemon(repo)
            hello = h.grove(source, "coord", "hello")
            h.check(hello == h.grove(target, "coord", "hello"), "worktrees share stable repository identity")
            h.check(not (repo / ".grove").exists(), "coordination store is outside worktree contents")
            acceptance = "account for runtime, AGENTS.md, configuration, tests and documentation"
            for cwd, identity in ((source, "codex:a"), (target, "codex:b")):
                h.grove(cwd, "coord", "register", "--session", identity, "--task", "port entire requested branch", "--acceptance", acceptance)
            view = h.grove(target, "coord", "sync", "--session", "codex:b")
            h.check(len(view["peers"]) == 2, "two worktrees discover each other")
            h.check(len({p["observation"]["worktree_id"] for p in view["peers"]}) == 2, "worktree identities differ independently of branches")
            h.grove(source, "coord", "propose", "--session", "codex:a", "--decision-id", "whole-transfer", "--body", "Include repository policy changes in every whole-branch handoff.", "--provenance", "fixture operator requirement")
            proposal = h.grove(target, "coord", "sync", "--session", "codex:b")
            h.check(not proposal["current_decisions"] and proposal["proposals"][0]["revision"] == 1, "proposed decisions are separate from instructions")
            h.grove(repo, "coord", "approve", "--decision-id", "whole-transfer", "--revision", "1", "--authority", "disposable acceptance operator")
            h.grove(target, "coord", "ack-decision", "--session", "codex:b", "--decision-id", "whole-transfer", "--revision", "1")

            with socket.socket() as port_socket:
                port_socket.bind(("127.0.0.1", 0))
                port = port_socket.getsockname()[1]
            gateway = h.launch(repo, "coord", "serve", "--bind", f"127.0.0.1:{port}")
            url = f"http://127.0.0.1:{port}/coord"
            remote = h.env.copy()
            remote.update(GROVE_COORD_URL=url, GROVE_REPOSITORY_ID=hello["repository_id"])
            for _ in range(150):
                with socket.socket() as s:
                    if s.connect_ex(("127.0.0.1", port)) == 0:
                        break
                h.check(gateway.poll() is None, "gateway stays alive")
                time.sleep(.02)
            # Independent clone: no common .git directory, no shared client DB.
            clone = root / "isolated-clone"
            h.git(root, "clone", str(repo), str(clone))
            h.git(clone, "config", "user.email", "grove-acceptance@example.test")
            h.git(clone, "config", "user.name", "Grove acceptance fixture")
            h.grove(clone, "coord", "register", "--session", "codex:cloud-client", "--task", "isolated client smoke", "--acceptance", "recover shared peers", env=remote)
            cloud = h.grove(clone, "coord", "sync", "--session", "codex:cloud-client", env=remote)
            h.check(len(cloud["peers"]) == 3, "authenticated isolated-clone client discovers service peers")
            h.check(not (clone / ".git" / "grove").exists(), "remote clients create no competing coordination database")
            h.check(next(p for p in cloud["peers"] if p["session_id"] == "codex:cloud-client")["observation"]["verification"] == "client_reported", "remote Git evidence is labeled as reported")
            wrong = remote.copy(); wrong["GROVE_REPOSITORY_ID"] = str(uuid.uuid4())
            h.grove(clone, "coord", "hello", env=wrong, ok=False)
            bad = remote.copy(); bad["GROVE_COORD_TOKEN"] = "wrong"
            h.grove(clone, "coord", "hello", env=bad, ok=False)
            partial = remote.copy(); partial.pop("GROVE_COORD_URL")
            h.grove(clone, "coord", "hello", env=partial, ok=False)
            unsafe = remote.copy(); unsafe["GROVE_COORD_URL"] = "http://example.test/coord"
            h.grove(clone, "coord", "hello", env=unsafe, ok=False)

            started = time.perf_counter()
            sent = h.grove(source, "coord", "send", "--session", "codex:a", "--to", "codex:b", "--kind", "request", "--body", "port all requested changes including AGENTS.md", "--key", "request-1", env=remote)
            retry = h.grove(source, "coord", "send", "--session", "codex:a", "--to", "codex:b", "--kind", "request", "--body", "port all requested changes including AGENTS.md", "--key", "request-1", env=remote)
            inbox = h.grove(target, "coord", "sync", "--session", "codex:b", env=remote)
            delivery_ms = (time.perf_counter() - started) * 1000
            h.check(retry["message_id"] == sent["message_id"] and len(inbox["inbox"]) == 1, "retry has one durable message effect")
            h.grove(target, "coord", "checkpoint", "--session", "codex:b", "--cursor", str(inbox["next_cursor"]), env=remote)
            h.grove(repo, "daemon", "stop")
            daemon.wait(timeout=10)
            daemon = h.daemon(repo)
            recovered = h.grove(target, "coord", "sync", "--session", "codex:b", env=remote)
            h.check(recovered["inbox"][0]["message_id"] == sent["message_id"], "restart preserves unacknowledged inbox beyond checkpoint")
            h.check(recovered["current_decisions"][0]["acknowledged"], "restart preserves exact decision revision acknowledgement")
            receipt = h.grove(target, "coord", "ack-message", "--session", "codex:b", "--message-id", sent["message_id"], env=remote)
            h.grove(target, "coord", "ack-message", "--session", "codex:b", "--message-id", sent["message_id"], env=remote)
            handoff = h.grove(target, "coord", "send", "--session", "codex:b", "--to", "codex:a", "--kind", "handoff", "--body", "policy omission identified; completion is blocked", "--key", "handoff-1", env=remote)
            h.grove(source, "coord", "ack-message", "--session", "codex:a", "--message-id", handoff["message_id"], env=remote)

            # Scope inventory exists before the deliberately partial transfer.
            captured = h.grove(source, "coord", "capture-transfer", "--session", "codex:a", "--transfer-id", "entire-pr", "--source", "source", "--base", base, "--target", "target", env=remote)
            h.check(len(captured["manifest"]["items"]) == 5, "inventory includes runtime, policy, config, tests and docs")
            (target / "runtime.js").write_text((source / "runtime.js").read_text())
            h.git(target, "add", "runtime.js"); h.git(target, "commit", "-m", "deliberately incomplete runtime-only transfer")
            h.grove(target, "coord", "heartbeat", "--session", "codex:b", env=remote)
            incomplete = h.grove(source, "coord", "check-transfer", "--session", "codex:a", "--transfer-id", "entire-pr", env=remote)
            h.check("AGENTS.md" in incomplete["unresolved"] and len(incomplete["unresolved"]) == 4, "runtime-only transfer exposes all omitted scope")
            h.check(not incomplete["complete_at_observed_commits"], "partial transfer cannot report completion")
            for item in captured["manifest"]["items"]:
                (target / item["path"]).write_bytes((source / item["path"]).read_bytes())
            h.git(target, "add", "."); h.git(target, "commit", "-m", "complete requested fixture scope")
            h.grove(target, "coord", "heartbeat", "--session", "codex:b", env=remote)
            complete = h.grove(source, "coord", "check-transfer", "--session", "codex:a", "--transfer-id", "entire-pr", env=remote)
            h.check(not complete["unresolved"] and not complete["complete_at_observed_commits"], "matching trees still require acceptance review")
            review = {"source_commit": complete["source_commit"], "base_commit": complete["base_commit"], "target_commit": complete["target_commit"], "reviewer": "disposable acceptance operator", "items": [], "criteria": [{"index": 0, "accepted": True, "evidence": "fixture compares all five paths, including policy; semantic review is simulated"}]}
            review_file = root / "review.json"; review_file.write_text(json.dumps(review))
            reviewed = h.grove(source, "coord", "review-transfer", "--transfer-id", "entire-pr", "--review-file", str(review_file), env=remote)
            h.check(reviewed["complete_at_observed_commits"], "complete scope plus explicit fixture review passes")
            (target / "new.txt").write_text("later target commit\n"); h.git(target, "add", "new.txt"); h.git(target, "commit", "-m", "invalidate completion evidence")
            h.grove(target, "coord", "heartbeat", "--session", "codex:b", env=remote)
            changed = h.grove(source, "coord", "get-transfer", "--session", "codex:a", "--transfer-id", "entire-pr", env=remote)
            h.check(not changed["status"]["complete_at_observed_commits"] and changed["status"]["known_ref_changes"], "peer HEAD update invalidates old completion evidence")
            rechecked = h.grove(source, "coord", "check-transfer", "--session", "codex:a", "--transfer-id", "entire-pr", env=remote)
            h.check(not rechecked["review_valid"], "changed target invalidates semantic review")

            claim = h.grove(source, "coord", "claim", "--session", "codex:a", "--path", "runtime.js", "--ttl-seconds", "5", env=remote)
            other = h.grove(target, "coord", "claim", "--session", "codex:b", "--path", "runtime.js", "--ttl-seconds", "5", env=remote)
            h.check(claim["claim_id"] in other["overlaps"], "overlapping assignments are visible")
            time.sleep(5.1)
            recovered_claim = h.grove(source, "coord", "claim", "--session", "codex:a", "--path", "runtime.js", env=remote)
            h.check(recovered_claim["epoch"] == 2, "expired claim recovers with new epoch")
            h.grove(source, "coord", "release-claim", "--session", "codex:a", "--claim-id", claim["claim_id"], "--epoch", "1", env=remote, ok=False)
            h.grove(source, "coord", "release-claim", "--session", "codex:a", "--claim-id", claim["claim_id"], "--epoch", "2", env=remote)

            mcp = mcp_roundtrip(h, source, remote)
            # Concurrent retry storm must still enqueue exactly one message.
            def send_retry(_):
                return h.grove(source, "coord", "send", "--session", "codex:a", "--to", "codex:b", "--kind", "progress", "--body", "concurrent deduplication test", "--key", "storm", env=remote)
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                messages = list(pool.map(send_retry, range(16)))
            h.check(len({m["message_id"] for m in messages}) == 1, "concurrent delivery retries have one message effect")
            final = h.grove(target, "coord", "sync", "--session", "codex:b", env=remote)
            h.check(len(final["inbox"]) == 1, "no missed or duplicated pending messages")
            h.grove(repo, "daemon", "stop"); daemon.wait(timeout=10)
            return {"assertions": h.assertions, "service_delivery_roundtrip_ms": round(delivery_ms, 2), "service_first_read_delay_ms": receipt["service_read_at"]-receipt["enqueued_at"], "explicit_ack_delay_ms": receipt["acknowledged_at"]-receipt["service_read_at"], "missed_messages": 0, "duplicate_message_effects": 0, "incomplete_transfer_unresolved": incomplete["unresolved"], "restart_recovery": "passed", "mcp": mcp, "transport": "authenticated loopback HTTP from isolated clone; not a cross-host/cloud proof", "codex_models": "not used; see separate real-session demonstration", "semantic_acceptance_review": "simulated fixture operator", "false_alarms": "not measured against independently labeled real workloads", "manual_intervention": "zero within scripted fixture; real service hosting and credentials require operator setup"}
        finally:
            h.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path(__file__).resolve().parents[1]/"target/debug/grove")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run(args.binary.resolve())
    serialized = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(serialized)
    print(serialized)
