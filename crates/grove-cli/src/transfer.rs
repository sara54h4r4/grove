//! Read-only Git evidence. An inventory never drops instruction/configuration
//! files and never treats a successful build as proof of complete scope.
use grove_lib::coordination::*;
use std::{collections::BTreeMap, path::Path, process::Command};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn git(repo: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .current_dir(repo)
        .arg("--literal-pathspecs")
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "Git evidence command failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(output.stdout)
}
pub fn git_text(repo: &Path, args: &[&str]) -> Result<String> {
    Ok(String::from_utf8(git(repo, args)?)?.trim().into())
}

pub fn resolve(repo: &Path, reference: &str) -> Result<String> {
    if reference.is_empty() || reference.starts_with('-') || reference.contains(['\0', '\n', '\r'])
    {
        return Err("invalid commit reference".into());
    }
    git_text(
        repo,
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{reference}^{{commit}}"),
        ],
    )
}
fn stable_ref(repo: &Path, reference: &str) -> Result<String> {
    resolve(repo, reference)?;
    let name = git_text(repo, &["rev-parse", "--symbolic-full-name", reference])?;
    if name.is_empty() {
        resolve(repo, reference)
    } else {
        Ok(name)
    }
}

pub fn entry(repo: &Path, commit: &str, path: &str) -> Result<Option<TreeEntry>> {
    let bytes = git(repo, &["ls-tree", "-z", commit, "--", path])?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let header = bytes
        .split(|b| *b == b'\t')
        .next()
        .ok_or("invalid Git tree entry")?;
    let text = std::str::from_utf8(header)?;
    let mut words = text.split(' ');
    let mode = words.next().ok_or("missing tree mode")?.to_owned();
    let _kind = words.next();
    let oid = words.next().ok_or("missing tree object")?.to_owned();
    Ok(Some(TreeEntry { mode, oid }))
}

pub fn capture(
    repo: &Path,
    source: &str,
    base: &str,
    target: &str,
    pr_url: Option<String>,
) -> Result<TransferManifest> {
    let source_commit = resolve(repo, source)?;
    let base_commit = resolve(repo, base)?;
    let initial_target_commit = resolve(repo, target)?;
    let ancestry = Command::new("git")
        .current_dir(repo)
        .args(["merge-base", "--is-ancestor", &base_commit, &source_commit])
        .status()?;
    if !ancestry.success() {
        return Err(
            "source base is not an ancestor of source; choose the PR merge base explicitly".into(),
        );
    }
    let bytes = git(
        repo,
        &[
            "diff",
            "--name-only",
            "-z",
            "--no-renames",
            "--no-ext-diff",
            &base_commit,
            &source_commit,
            "--",
        ],
    )?;
    let mut items = Vec::new();
    for raw in bytes.split(|b| *b == 0).filter(|b| !b.is_empty()) {
        let path = std::str::from_utf8(raw)?.to_owned();
        if !valid_scope(&path) || path == "." {
            return Err("transfer contains a path outside the supported literal UTF-8 path contract; inventory was not saved".into());
        }
        items.push(TransferItem {
            before: entry(repo, &base_commit, &path)?,
            after: entry(repo, &source_commit, &path)?,
            initial_target: entry(repo, &initial_target_commit, &path)?,
            path,
        });
    }
    Ok(TransferManifest {
        source_ref: stable_ref(repo, source)?,
        base_ref: stable_ref(repo, base)?,
        target_ref: stable_ref(repo, target)?,
        source_commit,
        base_commit,
        initial_target_commit,
        pr_url,
        items,
    })
}

pub fn observe(repo: &Path, manifest: &TransferManifest) -> Result<TransferObservation> {
    let source_commit = resolve(repo, &manifest.source_ref)?;
    let base_commit = resolve(repo, &manifest.base_ref)?;
    let target_commit = resolve(repo, &manifest.target_ref)?;
    let mut target_entries = BTreeMap::new();
    for item in &manifest.items {
        target_entries.insert(item.path.clone(), entry(repo, &target_commit, &item.path)?);
    }
    let target_dirty = !git(
        repo,
        &["status", "--porcelain=v1", "-z", "--untracked-files=normal"],
    )?
    .is_empty();
    Ok(TransferObservation {
        source_commit,
        base_commit,
        target_commit,
        target_entries,
        target_dirty,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn command(repo: &Path, args: &[&str]) {
        git(repo, args).unwrap();
    }
    #[test]
    fn git_inventory_keeps_renames_deletions_binary_modes_and_literal_policy_paths() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path();
        command(repo, &["init", "-b", "main"]);
        command(repo, &["config", "user.name", "Grove fixture"]);
        command(repo, &["config", "user.email", "fixture@example.test"]);
        std::fs::write(repo.join("AGENTS.md"), "policy v1").unwrap();
        std::fs::write(repo.join("old-name.txt"), "rename").unwrap();
        std::fs::write(repo.join("deleted.txt"), "remove").unwrap();
        std::fs::write(repo.join("binary.dat"), [0, 1, 2]).unwrap();
        std::fs::write(repo.join("script.sh"), "fixture").unwrap();
        command(repo, &["add", "."]);
        command(repo, &["commit", "-m", "base"]);
        let base = resolve(repo, "HEAD").unwrap();
        command(repo, &["mv", "old-name.txt", "new-name.txt"]);
        std::fs::remove_file(repo.join("deleted.txt")).unwrap();
        std::fs::write(repo.join("AGENTS.md"), "policy v2").unwrap();
        std::fs::write(repo.join("binary.dat"), [0, 3, 4]).unwrap();
        std::fs::write(repo.join("literal[policy].md"), "literal pathspec").unwrap();
        command(repo, &["add", "."]);
        command(repo, &["update-index", "--chmod=+x", "script.sh"]);
        command(repo, &["commit", "-m", "whole scope"]);
        let manifest = capture(repo, "HEAD", &base, &base, None).unwrap();
        let item = |path: &str| {
            manifest
                .items
                .iter()
                .find(|item| item.path == path)
                .unwrap()
        };
        assert!(item("old-name.txt").after.is_none());
        assert!(item("new-name.txt").before.is_none());
        assert!(item("deleted.txt").after.is_none());
        assert_ne!(item("binary.dat").before, item("binary.dat").after);
        assert_eq!(item("script.sh").after.as_ref().unwrap().mode, "100755");
        assert_eq!(item("script.sh").before.as_ref().unwrap().mode, "100644");
        assert!(item("literal[policy].md").after.is_some());
        assert_ne!(item("AGENTS.md").before, item("AGENTS.md").after);
        assert_eq!(manifest.items.len(), 7);
        assert!(resolve(repo, "--help").is_err());
    }
}
