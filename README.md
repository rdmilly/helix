# ⚡ Helix Cortex

> Personal AI intelligence platform — context capture, compression, knowledge graph, and MCP tooling.

[![README](https://github.com/rdmilly/helix/actions/workflows/readme.yml/badge.svg)](https://github.com/rdmilly/helix/actions)
![Containers](https://img.shields.io/badge/containers-24-blue?style=flat-square)
![Chunks](https://img.shields.io/badge/chunks-9434-purple?style=flat-square)
![Sessions](https://img.shields.io/badge/sessions-490-green?style=flat-square)
![Decisions](https://img.shields.io/badge/decisions-694-orange?style=flat-square)

**[🌐 Live Dashboard →](https://helixmaster.millyweb.com)**

---

## What is Helix?

Helix is a self-hosted AI intelligence layer built on top of LLMs. It captures every session, extracts decisions and entities, compresses context, and makes everything searchable — so Claude always knows what you've built, decided, and why.

Built entirely in public. Every commit is a real build session.

## Architecture

```
Claude → mcp-front → provision-filter → mcp-provisioner → helix-mcp → cortex
                                                                        ↓
                                                         Postgres · ChromaDB · MinIO
                                                         KG · KB · Scheduler · Observer
```

## Live Stats

| Metric | Count |
|--------|-------|
| 🧠 Conversation chunks | 9,434 |
| 🐳 Containers running | 24 |
| 💬 Sessions indexed | 490 |
| 🎯 Decisions recorded | 694 |
| 🔗 KG entities | 495 |
| 📄 KB documents | 171 |

> Stats auto-update hourly via GitHub Actions pulling from live Cortex API.

## Roadmap

| Phase | Status | Description |
|-------|--------|-------------|
| P1 Foundation | ✅ Complete | Cortex, Postgres, ChromaDB, event bus |
| P2 Ingestion | ✅ Complete | Conversation ingester, FTS5 indexing |
| P3 Intelligence | ✅ Complete | Haiku reconciler, KG chain, journal |
| P4 Gates | ✅ Complete | Session state, assembler, cortex_act |
| P5 Assembler | ✅ Complete | Spec gen, prestage, scaffold |
| P6 GitHub OAuth | 🔨 Building | Repo connect, auto-scanner, per-user git sync |

## Key Services

- **helix-cortex** — FastAPI backend, REST + MCP tools (port 9050)
- **helix-mcp** — Single MCP surface for Claude (port 9096)
- **Scheduler** — 7 background jobs (compression, patterns, backup, reconciler, ingester)
- **Intelligence Chain** — Haiku extracts decisions → KG → journal → ADRs
- **Assembler** — Prestage candidates, spec generation, scaffold suggestions
- **Git Sync** — Every `helix_file_write` auto-commits to GitHub with scan pipeline

## Stack

`Python` `FastAPI` `PostgreSQL` `ChromaDB` `MinIO` `Docker` `MCP` `Claude API`

## Related

- [MemBrain](https://github.com/rdmilly/membrain) — Browser extension for AI conversation capture
- [helixmaster.millyweb.com](https://helixmaster.millyweb.com) — Live build dashboard

---

*README auto-updated by GitHub Actions on every push · [helixmaster.millyweb.com](https://helixmaster.millyweb.com)*
