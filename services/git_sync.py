"""Git Sync Service

Subscribes to file.written events and auto-commits changed files
to their git repo with the session context as the commit message.

Only fires when:
  - The file path is inside a git repo
  - The repo has a remote origin configured
  - GITHUB_TOKEN is available

Commit format: "[helix] <session_id>: <path>"
"""
import subprocess
import logging
import os
from pathlib import Path

log = logging.getLogger("helix.git_sync")

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GIT_AUTHOR_NAME = "Helix Cortex"
GIT_AUTHOR_EMAIL = "helix@millyweb.com"

# Repos we know about + their remote URLs
KNOWN_REPOS = {
    "/opt/projects/memory-ext": "https://rdmilly:{token}@github.com/rdmilly/membrain.git",
    "/opt/projects/helix": "https://rdmilly:{token}@github.com/rdmilly/helix.git",
}


def _find_repo_root(path: str) -> str | None:
    """Walk up from path to find .git directory."""
    p = Path(path)
    if p.is_file():
        p = p.parent
    for parent in [p] + list(p.parents):
        if (parent / ".git").exists():
            return str(parent)
    return None


def _git(repo_root: str, *args, token: str = "") -> tuple[int, str, str]:
    """Run a git command in a repo."""
    env = os.environ.copy()
    env["GIT_AUTHOR_NAME"] = GIT_AUTHOR_NAME
    env["GIT_AUTHOR_EMAIL"] = GIT_AUTHOR_EMAIL
    env["GIT_COMMITTER_NAME"] = GIT_AUTHOR_NAME
    env["GIT_COMMITTER_EMAIL"] = GIT_AUTHOR_EMAIL
    if token:
        env["GIT_ASKPASS"] = "echo"
        env["GIT_TOKEN"] = token
    result = subprocess.run(
        ["git"] + list(args),
        cwd=repo_root,
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )
    return result.returncode, result.stdout.strip(), result.stderr.strip()


def auto_commit(path: str, session_id: str = "helix", context: str = "") -> dict:
    """Auto-commit a changed file to its git repo.

    Returns dict with status, committed (bool), and any error.
    """
    token = GITHUB_TOKEN
    if not token:
        return {"status": "skipped", "reason": "GITHUB_TOKEN not set"}

    repo_root = _find_repo_root(path)
    if not repo_root:
        return {"status": "skipped", "reason": "not in a git repo"}

    # Check if there are changes to commit
    rc, out, err = _git(repo_root, "status", "--porcelain", path)
    if rc != 0 or not out.strip():
        return {"status": "skipped", "reason": "no changes to commit"}

    # Stage the file
    rc, out, err = _git(repo_root, "add", path)
    if rc != 0:
        return {"status": "error", "reason": f"git add failed: {err}"}

    # Build commit message
    rel_path = Path(path).relative_to(repo_root) if repo_root in path else path
    msg = f"[helix] {rel_path}"
    if context:
        msg += f" — {context[:80]}"
    elif session_id and session_id != "helix":
        msg += f" ({session_id[:12]})"

    # Commit
    rc, out, err = _git(repo_root, "commit", "-m", msg)
    if rc != 0:
        log.warning(f"git commit failed: {err}")
        return {"status": "error", "reason": err}

    log.info(f"[GitSync] Committed: {msg}")

    # Push — set remote URL with token if we know it
    remote_url = None
    for known_root, url_template in KNOWN_REPOS.items():
        if repo_root.startswith(known_root):
            remote_url = url_template.format(token=token)
            break

    if remote_url:
        # Set remote URL with embedded token (never stored in repo)
        _git(repo_root, "remote", "set-url", "origin", remote_url)
        rc, out, err = _git(repo_root, "push", "origin", "HEAD")
        # Reset remote URL to token-free version after push
        for known_root, url_template in KNOWN_REPOS.items():
            if repo_root.startswith(known_root):
                clean_url = url_template.replace("{token}@", "").format(token="")
                _git(repo_root, "remote", "set-url", "origin", clean_url)
                break
        if rc != 0:
            log.warning(f"[GitSync] Push failed (commit saved locally): {err}")
            return {"status": "committed", "pushed": False, "commit_msg": msg, "push_err": err}
        log.info(f"[GitSync] Pushed to GitHub")
        return {"status": "committed", "pushed": True, "commit_msg": msg}
    else:
        return {"status": "committed", "pushed": False, "commit_msg": msg, "reason": "no remote configured"}
