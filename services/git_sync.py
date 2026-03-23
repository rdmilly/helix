"""Git Sync Service

On every helix_file_write, creates a feature branch via the GitHub API,
commits the file using the Contents API (no local branch switching),
opens a pull request, squash-merges it, then deletes the branch.

The GitHub Contents API approach avoids all local git state issues
(untracked files, dirty working tree, branch conflicts).

Workflow per file write:
  1. Get base SHA from main
  2. Create feature branch: helix/<timestamp>-<short-path>
  3. Upsert file on that branch via Contents API
  4. Open PR: "[helix] <description>"
  5. Squash-merge PR -> main
  6. Delete branch

Falls back to direct push if anything fails.
"""
import logging
import os
import re
import time
import base64
import subprocess
import requests
from pathlib import Path
from datetime import datetime, timezone

log = logging.getLogger("helix.git_sync")

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GIT_AUTHOR_NAME = "Helix Cortex"
GIT_AUTHOR_EMAIL = "helix@millyweb.com"
GH_API = "https://api.github.com"

KNOWN_REPOS = {
    "/opt/projects/memory-ext": {"remote": "rdmilly/membrain", "branch": "main"},
    "/opt/projects/helix":      {"remote": "rdmilly/helix",    "branch": "main"},
}


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _sanitize(s: str) -> str:
    s = re.sub(r'[^a-zA-Z0-9._-]', '-', s)
    return re.sub(r'-{2,}', '-', s).strip('-')[:40]


def _find_repo_root(path: str):
    p = Path(path)
    if p.is_file():
        p = p.parent
    for parent in [p] + list(p.parents):
        if (parent / ".git").exists():
            return str(parent)
    return None


def _git(repo_root: str, *args) -> tuple:
    env = os.environ.copy()
    env["GIT_AUTHOR_NAME"] = GIT_AUTHOR_NAME
    env["GIT_AUTHOR_EMAIL"] = GIT_AUTHOR_EMAIL
    env["GIT_COMMITTER_NAME"] = GIT_AUTHOR_NAME
    env["GIT_COMMITTER_EMAIL"] = GIT_AUTHOR_EMAIL
    r = subprocess.run(["git"] + list(args), cwd=repo_root,
                       capture_output=True, text=True, env=env, timeout=30)
    return r.returncode, r.stdout.strip(), r.stderr.strip()


# ── GitHub API helpers ────────────────────────────────────────────

def _get_sha(repo: str, branch: str, h: dict) -> str | None:
    r = requests.get(f"{GH_API}/repos/{repo}/git/ref/heads/{branch}", headers=h, timeout=10)
    return r.json()["object"]["sha"] if r.status_code == 200 else None


def _create_branch(repo: str, branch: str, sha: str, h: dict) -> bool:
    r = requests.post(f"{GH_API}/repos/{repo}/git/refs", headers=h,
                      json={"ref": f"refs/heads/{branch}", "sha": sha}, timeout=10)
    return r.status_code in (200, 201, 422)


def _get_file_sha(repo: str, path: str, branch: str, h: dict) -> str | None:
    """Get the blob SHA of a file on a branch (needed for updates)."""
    r = requests.get(f"{GH_API}/repos/{repo}/contents/{path}",
                     params={"ref": branch}, headers=h, timeout=10)
    return r.json().get("sha") if r.status_code == 200 else None


def _upsert_file(repo: str, path: str, content: str, branch: str,
                 message: str, h: dict) -> bool:
    """Create or update a file on a branch via the Contents API."""
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode()).decode(),
        "branch": branch,
        "committer": {"name": GIT_AUTHOR_NAME, "email": GIT_AUTHOR_EMAIL},
    }
    # If file already exists on this branch, include its SHA
    existing_sha = _get_file_sha(repo, path, branch, h)
    if existing_sha:
        payload["sha"] = existing_sha
    r = requests.put(f"{GH_API}/repos/{repo}/contents/{path}",
                     headers=h, json=payload, timeout=15)
    return r.status_code in (200, 201)


def _open_pr(repo: str, head: str, base: str, title: str, body: str, h: dict) -> int | None:
    r = requests.post(f"{GH_API}/repos/{repo}/pulls", headers=h,
                      json={"title": title, "head": head, "base": base, "body": body},
                      timeout=10)
    return r.json()["number"] if r.status_code in (200, 201) else None


def _merge_pr(repo: str, pr: int, title: str, h: dict) -> bool:
    time.sleep(2)
    r = requests.put(f"{GH_API}/repos/{repo}/pulls/{pr}/merge", headers=h,
                     json={"merge_method": "squash", "commit_title": title},
                     timeout=15)
    return r.status_code == 200


def _delete_branch(repo: str, branch: str, h: dict):
    requests.delete(f"{GH_API}/repos/{repo}/git/refs/heads/{branch}",
                    headers=h, timeout=10)


def _pr_body(path: str, session_id: str, context: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"## `{Path(path).name}`",
        "",
        f"**Path:** `{path}`",
        f"**Session:** `{session_id[:16]}`",
        f"**Time:** {ts}",
    ]
    if context:
        lines += ["", f"**Context:** {context[:200]}"]
    lines += ["", "---",
              "*Auto-generated by [Helix Cortex](https://helix.millyweb.com) write pipeline.*"]
    return "\n".join(lines)


# ── Main entry point ───────────────────────────────────────────────

def auto_commit(path: str, session_id: str = "helix", context: str = "") -> dict:
    """Commit file via GitHub API: branch -> PR -> squash merge.

    Uses the Contents API for the file commit so no local branch
    switching is needed — works cleanly regardless of working tree state.
    """
    token = GITHUB_TOKEN
    if not token:
        return {"status": "skipped", "reason": "GITHUB_TOKEN not set"}

    repo_root = _find_repo_root(path)
    if not repo_root:
        return {"status": "skipped", "reason": "not in a git repo"}

    repo_config = next(
        (cfg for root, cfg in KNOWN_REPOS.items() if repo_root.startswith(root)),
        None
    )
    if not repo_config:
        return {"status": "skipped", "reason": "repo not in KNOWN_REPOS"}

    gh_repo   = repo_config["remote"]
    base      = repo_config["branch"]
    h         = _headers(token)

    # Relative path within the repo (what GitHub uses)
    try:
        rel_path = str(Path(path).relative_to(repo_root))
    except ValueError:
        rel_path = Path(path).name

    ts_slug   = datetime.now(timezone.utc).strftime("%m%d-%H%M")
    short     = _sanitize(rel_path)
    branch    = f"helix/{ts_slug}-{short}"

    commit_msg = f"[helix] {rel_path}"
    if context and context not in ("helix", "workbench"):
        commit_msg += f" — {context[:60]}"
    elif session_id and session_id not in ("helix", "workbench"):
        commit_msg += f" ({session_id[:12]})"

    try:
        # Read current file content from disk
        content = Path(path).read_text(encoding="utf-8", errors="replace")

        # 1. Get base SHA
        base_sha = _get_sha(gh_repo, base, h)
        if not base_sha:
            raise ValueError(f"Cannot get SHA for {base}")

        # 2. Create feature branch
        _create_branch(gh_repo, branch, base_sha, h)

        # 3. Upsert file on feature branch via Contents API
        ok = _upsert_file(gh_repo, rel_path, content, branch, commit_msg, h)
        if not ok:
            raise ValueError("Contents API upsert failed")

        # 4. Open PR
        pr_number = _open_pr(gh_repo, branch, base, commit_msg,
                             _pr_body(path, session_id, context), h)
        if not pr_number:
            raise ValueError("PR creation failed")

        # 5. Merge PR
        merged = _merge_pr(gh_repo, pr_number, commit_msg, h)
        _delete_branch(gh_repo, branch, h)

        if merged:
            log.info(f"[GitSync] PR #{pr_number} merged → {base}: {commit_msg}")
            return {"status": "merged", "pr": pr_number,
                    "branch": branch, "commit_msg": commit_msg}
        else:
            log.warning(f"[GitSync] PR #{pr_number} open but merge failed")
            return {"status": "pr_open", "pr": pr_number,
                    "branch": branch, "commit_msg": commit_msg}

    except Exception as e:
        log.warning(f"[GitSync] PR workflow failed ({e}), falling back to direct push")
        try:
            remote_url = f"https://rdmilly:{token}@github.com/{gh_repo}.git"
            _git(repo_root, "remote", "set-url", "origin", remote_url)
            _git(repo_root, "add", path)
            _git(repo_root, "commit", "-m", commit_msg)
            rc, _, err = _git(repo_root, "push", "origin", f"HEAD:{base}")
            _git(repo_root, "remote", "set-url", "origin",
                 f"https://github.com/{gh_repo}.git")
            return ({"status": "committed", "pushed": True,
                     "commit_msg": commit_msg, "fallback": True}
                    if rc == 0 else {"status": "error", "reason": err})
        except Exception as e2:
            return {"status": "error", "reason": str(e2)}
