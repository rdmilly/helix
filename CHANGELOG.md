# Changelog

## 2026-03-23
- PR-based git workflow: every file write now creates a feature branch, opens a PR, and squash-merges to main
- Full project README with architecture diagram, stats, and stack
- Write pipeline: all 5 steps firing cleanly (git, kb_index, kg_extract, forge_version, scan)
- Fixed: reconciler f-string crash, observer JSONB truncation, kb upsert conflict, compression FTS conn
- GITHUB_TOKEN saved to Infisical + helix.env, live in container
