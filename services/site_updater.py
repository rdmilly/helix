"""Site Updater — subscriber to file.written

On every file write, fetches latest GitHub commits for both repos
and rewrites the dynamic commit feed section of helixmaster/index.html.

Updates:
  - Recent commits feed (last 10 across both repos)
  - Last updated timestamp in footer
"""
import logging
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("helix.site_updater")

HELIXMASTER = "/opt/projects/helixmaster/index.html"
GH_TOKEN = None  # loaded lazily from env
REPOS = [
    ("rdmilly/helix", "Helix"),
    ("rdmilly/membrain", "MemBrain"),
]


def _gh_token():
    global GH_TOKEN
    if not GH_TOKEN:
        import os
        GH_TOKEN = os.environ.get("GITHUB_TOKEN", "")
    return GH_TOKEN


def _fetch_commits(repo: str, n: int = 5) -> list:
    try:
        token = _gh_token()
        url = f"https://api.github.com/repos/{repo}/commits?per_page={n}"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        log.warning(f"GitHub fetch failed for {repo}: {e}")
        return []


def _build_commit_html(commits_by_repo: list) -> str:
    """Build HTML for the commit feed section."""
    items = []
    for repo, label, commit in commits_by_repo:
        msg = commit.get("commit", {}).get("message", "").split("\n")[0][:80]
        sha = commit.get("sha", "")[:7]
        url = commit.get("html_url", "#")
        date = commit.get("commit", {}).get("author", {}).get("date", "")[:10]
        items.append(
            f'<div class="commit-item">'
            f'<span class="commit-label">{label}</span> '
            f'<a href="{url}" target="_blank" class="commit-msg">{msg}</a> '
            f'<span class="commit-meta">{sha} · {date}</span>'
            f'</div>'
        )
    return "\n".join(items)


def update_site() -> dict:
    """Fetch latest commits and update helixmaster index.html."""
    if not Path(HELIIXMASTER if False else HELIXMASTER).exists():
        return {"status": "skipped", "reason": "helixmaster/index.html not found"}

    # Fetch commits from both repos
    all_commits = []
    for repo, label in REPOS:
        commits = _fetch_commits(repo, n=5)
        for c in commits:
            all_commits.append((repo, label, c))

    # Sort by date descending
    all_commits.sort(
        key=lambda x: x[2].get("commit", {}).get("author", {}).get("date", ""),
        reverse=True
    )
    recent = all_commits[:10]

    if not recent:
        return {"status": "skipped", "reason": "no commits fetched"}

    commit_html = _build_commit_html(recent)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with open(HELIXMASTER) as f:
        html = f.read()

    # Replace between markers
    start_marker = "<!-- COMMIT_FEED_START -->"
    end_marker = "<!-- COMMIT_FEED_END -->"

    if start_marker in html and end_marker in html:
        before = html.split(start_marker)[0]
        after = html.split(end_marker)[1]
        html = f"{before}{start_marker}\n{commit_html}\n{end_marker}{after}"
    else:
        # Markers not present yet — append feed before </body>
        feed_section = (
            f'\n<!-- COMMIT_FEED_START -->\n{commit_html}\n<!-- COMMIT_FEED_END -->\n'
        )
        html = html.replace("</body>", feed_section + "</body>")

    # Update last-updated timestamp
    if "<!-- LAST_UPDATED -->" in html:
        html = html.split("<!-- LAST_UPDATED -->")[0] + \
               f"<!-- LAST_UPDATED -->{now}<!-- /LAST_UPDATED -->" + \
               html.split("<!-- /LAST_UPDATED -->")[-1]

    with open(HELIXMASTER, "w") as f:
        f.write(html)

    log.info(f"[SiteUpdater] Updated helixmaster with {len(recent)} commits")
    return {"status": "updated", "commits": len(recent), "updated_at": now}


HELIXMASTER = "/opt/projects/helixmaster/index.html"
