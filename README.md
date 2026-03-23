# ⚡ Helix Cortex

> A self-hosted AI intelligence platform that gives LLMs persistent memory, context compression, and a living knowledge graph — built entirely in public.

[![Build](https://github.com/rdmilly/helix/actions/workflows/readme.yml/badge.svg)](https://github.com/rdmilly/helix/actions)
![Version](https://img.shields.io/badge/version-0.9.0-blue?style=flat-square)
![Self-Hosted](https://img.shields.io/badge/deployment-self--hosted-orange?style=flat-square)
![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)

**[🌐 Live Dashboard](https://helixmaster.millyweb.com)** · **[📡 API Docs](https://helix.millyweb.com/docs)**

---

## What is Helix?

LLMs forget everything between sessions. Helix fixes that.

It sits between you and your AI tools — capturing every conversation, extracting decisions and patterns, compressing context intelligently, and making everything instantly searchable. When you start a new chat, Helix automatically injects the most relevant history so the AI already knows what you've built, what you decided, and why.

Built by a solo developer over 6+ months of daily use. Every commit is a real build session — not a demo project.

---

## Architecture

```
 Browser / Claude.ai
 MemBrain Extension v0.5
 (intercepts turns, injects context)
        |
        v POST /api/v1/ext/ingest
        |
+-------+--------------------------------------------------+
|                   Helix Cortex v0.9                      |
|                                                          |
|  Ingestion Pipeline -> Intelligence Chain -> MCP Server  |
|  Vector Search      -> Knowledge Graph   -> Compression  |
|  (pgvector)            (Postgres)          (Haiku)       |
+----------------------------------------------------------+
        |                  |                  |
   PostgreSQL          ChromaDB           The Forge
   (structured)        (vectors)         (versioning)
```

---

## Core Features

### 🧠 Persistent Context Memory
- Captures every Claude.ai conversation turn via the MemBrain browser extension
- Chunks and indexes into PostgreSQL + pgvector for hybrid BM25 + semantic search
- Auto-injects the 3 most relevant past sessions into new chats at load time
- ~5 second pipeline lag from conversation turn to searchable

### 🔍 Intelligence Extraction
- Haiku-powered reconciler extracts decisions, patterns, risks, and assumptions from every session
- Builds a knowledge graph of entities (projects, services, containers, domains) and relationships
- Auto-generates ADR (Architecture Decision Record) files for every architectural choice
- 9,200+ archived intelligence items across 6 months of daily building

### 🔧 MCP Server (20 tools)
Exposes everything as MCP tools Claude can call directly in any session:

| Tool | Purpose |
|------|---------|
| `helix_file_write` | Unified write pipeline — disk + git + KB + KG + versioning |
| `helix_search_conversations` | Semantic + BM25 search across all past sessions |
| `helix_search_kb` | Search the living knowledge base |
| `helix_file_read` | Read any file from VPS with metadata |
| `archive_record` | Write intelligence items to structured archive |
| `entity_upsert` | Create/update knowledge graph entities |
| `relationship_create` | Link entities in the knowledge graph |
| + 13 more | Exchanges, nudges, patterns, synapse, admin |

### 📁 Unified File Write Pipeline
Every `helix_file_write` call triggers a 5-step async pipeline:
1. **Write** — file to disk on VPS
2. **Git sync** — feature branch → PR → auto-merge to main
3. **KB index** — markdown/config files indexed for full-text search
4. **KG extract** — entities extracted and wired into knowledge graph
5. **Forge version** — MinIO-backed file versioning via The Forge

### 🗜️ Context Compression
- Per-session compression using a learned dictionary built from your actual vocabulary
- Reduces token usage by 30-60% on long conversations
- Learns your abbreviation and terminology patterns over time

---

## Stack

| Layer | Technology |
|-------|------------|
| API server | FastAPI + Uvicorn |
| Database | PostgreSQL 16 + pgvector extension |
| Vector search | ChromaDB + pgvector hybrid retrieval |
| File versioning | MinIO (Garage S3) via The Forge |
| Secrets management | Self-hosted Infisical |
| LLM calls | Anthropic Claude (Haiku for extraction, Sonnet for MCP) |
| Browser extension | MemBrain — Chrome extension capturing claude.ai turns |
| Deployment | Docker Compose on Hostinger VPS |
| Git automation | GitHub API — branch → PR → merge per file change |

---

## Live Stats

| Metric | Count |
|--------|-------|
| Conversation sessions indexed | 490+ |
| Intelligence items archived | 9,200+ |
| Decisions extracted | 1,161 |
| Patterns catalogued | 1,657 |
| Entities in knowledge graph | 491 |
| Code atoms in Forge catalog | 1,358 |
| Days building in public | 180+ |

---

## Project Structure

```
helix/
├── main.py                    # FastAPI app + MCP server
├── mcp_tools.py               # All 20 MCP tool definitions
├── routers/
│   ├── ext_ingest.py          # MemBrain ingestion + intelligence
│   ├── kb.py                  # Knowledge base search + indexing
│   ├── observer.py            # Tool call observer + action log
│   └── master_status.py       # System health dashboard
├── services/
│   ├── workbench.py           # Unified write pipeline orchestration
│   ├── intelligence_chain.py  # KG chain builder + ADR generation
│   ├── git_sync.py            # GitHub branch/PR/merge automation
│   ├── reconciler.py          # Haiku batch extraction scheduler
│   ├── scanner.py             # AST code atom extraction
│   ├── compression_profiles.py # Context compression engine
│   └── events/
│       ├── router.py          # Event type dispatch table
│       └── file_events.py     # file.written subscriber pipeline
└── working-kb/                # Living KB (auto-indexed on every write)
```

---

## Running Your Own Instance

This is a personal infrastructure project, not a packaged release yet. If you want to explore:

```bash
git clone https://github.com/rdmilly/helix.git
cd helix
cp .env.example .env   # fill in Anthropic key, Postgres DSN, etc.
docker compose up -d
```

Minimum: Docker, 2GB RAM, PostgreSQL with pgvector.

Full setup documentation is in progress.

---

## Building in Public

This repo is a live record of building real AI infrastructure from scratch — not a polished release. Commits are actual work sessions. Issues are real problems encountered and solved. The architecture evolves as understanding deepens.

If you're building something similar or just find this interesting: [@rdmilly on X](https://x.com/rdmilly)

---

*MIT License — use it, fork it, build on it.*
