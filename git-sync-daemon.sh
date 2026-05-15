#!/bin/bash
# ============================================================
# git-sync-daemon.sh — Universal Git Auto-Sync
# Watches ALL project and infra repos on this node.
# Any file change -> 10s debounce -> git add -A -> commit -> push
# Survives restarts. Runs as systemd service: git-sync.service
# ============================================================

GH_TOKEN="${GITHUB_TOKEN:-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}"
DEBOUNCE=10
LOGFILE=/var/log/git-sync.log
NODE=$(hostname)

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$NODE] $*" | tee -a $LOGFILE; }

# ---- Repos to watch on THIS node ----
# Each entry: "local_path|github_repo|branch"
VPS1_REPOS=(
  "/opt/projects/helix|rdmilly/helix|main"
  "/opt/projects/millyweb|rdmilly/millyweb|main"
  "/opt/projects/mw-lead-pipeline|rdmilly/mw-lead-pipeline|main"
  "/opt/projects/hostinger-watchdog|rdmilly/hostinger-watchdog|main"
  "/opt/projects/mcp-servers|rdmilly/mcp-servers|main"
  "/opt/projects/atrium|rdmilly/atrium|main"
  "/opt/projects/git-agent|rdmilly/git-agent|main"
  "/opt/projects/paving-agent|rdmilly/paving-agent|main"
  "/opt/projects/content-pipeline|rdmilly/content-pipeline|main"
  "/opt/stacks|rdmilly/millyweb-infra|main"
)

VPS2_REPOS=(
  "/opt/projects/mcp-provisioner|rdmilly/mcp-provisioner|main"
  "/opt/projects/atrium|rdmilly/atrium|main"
  "/opt/projects/memory-ext|rdmilly/memory-ext|main"
  "/opt/projects/mw-sites|rdmilly/mw-sites|main"
  "/opt/projects/millyweb-kb|rdmilly/millyweb-kb|main"
  "/opt/projects/printblocks|rdmilly/printblocks|main"
  "/opt/stacks|rdmilly/millyweb-infra|main"
)

# Pick repos for this node
if echo "$NODE" | grep -q 'vps2\|srv2\|81'; then
  REPOS=("${VPS2_REPOS[@]}")
else
  REPOS=("${VPS1_REPOS[@]}")
fi

EXCLUDE='(__pycache__|\.git|node_modules|\.pyc|postgres-data|neo4j-data|embeddings-cache|\.log|\.db|invoice-ninja/data|acme\.json)'

setup_remote() {
  local dir=$1 repo=$2
  cd "$dir" || return 1
  git config user.email "git-sync@millyweb.com"
  git config user.name "Git Sync Daemon"
  git remote set-url origin "https://rdmilly:${GH_TOKEN}@github.com/${repo}.git" 2>/dev/null
}

commit_and_push() {
  local dir=$1 repo=$2 branch=$3
  cd "$dir" || return
  [ -z "$(git status --porcelain)" ] && return
  local changed
  changed=$(git status --porcelain | grep -v '^??' | head -3 | awk '{print $2}' | tr '\n' ' ')
  [ -z "$changed" ] && changed=$(git status --porcelain | head -1 | awk '{print $2}')
  git add -A
  if git commit -m "sync: $changed" --quiet; then
    git push origin "$branch" --quiet && log "Pushed $repo: $changed" || log "Push failed: $repo"
  fi
}

log "Starting git-sync-daemon on $NODE — watching ${#REPOS[@]} repos"

for ENTRY in "${REPOS[@]}"; do
  IFS='|' read -r DIR REPO BRANCH <<< "$ENTRY"
  if [ ! -d "$DIR/.git" ]; then
    log "Skip $DIR (not a git repo)"
    continue
  fi
  setup_remote "$DIR" "$REPO"
  log "Watching: $DIR -> github.com/$REPO ($BRANCH)"
  (
    while inotifywait -r -e modify,create,delete,move \
      --exclude "$EXCLUDE" "$DIR" -q 2>/dev/null; do
      sleep "$DEBOUNCE"
      commit_and_push "$DIR" "$REPO" "$BRANCH"
    done
  ) &
done

wait
