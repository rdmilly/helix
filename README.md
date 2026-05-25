# ⚡ Helix Cortex

> A self-hosted AI intelligence platform that gives LLMs persistent memory, context compression, and a living knowledge graph — built entirely in public by a solo developer.

**In plain English:** AI tools forget everything between sessions. Helix fixes that — it captures every conversation, extracts decisions and context, and automatically injects the right history into every new chat. Built from scratch over 6 months of daily use. Not a demo — production infrastructure.

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
 MemBrain Extension
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

## Stack

`PostgreSQL` `pgvector` `Neo4j` `Redis` `FastMCP` `Python` `Docker`

Self-hosted on a 2-node VPS cluster managed by a custom MCP provisioner with 45+ servers and 697 tools.

---

## Builder

Ryan Milly — operator turned self-taught AI infrastructure developer. Previously founded and scaled a service business to $432K revenue before pivoting to build this stack from scratch.

[ryanmilly.com](https://ryanmilly.com) · [LinkedIn](https://linkedin.com/in/rdmilly)
