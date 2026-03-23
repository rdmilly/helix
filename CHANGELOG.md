# Changelog

## 2026-03-23
- PR-based git workflow: every file write creates a feature branch, opens a PR, and squash-merges to main
- Full project README with architecture diagram, stats, and stack
- Write pipeline: all 5 steps firing cleanly (git, kb_index, kg_extract, forge_version, scan)
- Fixed: reconciler f-string crash, observer JSONB truncation, kb upsert conflict, compression FTS conn
- GITHUB_TOKEN stored in Infisical at /helix/sensitive — auto-injected on every container start
- git added to Dockerfile so PR workflow survives image rebuilds
- Rewrote git_sync to use GitHub Contents API — no local branch switching needed
- PR workflow test: branch → PR → squash merge → branch cleanup ✅
