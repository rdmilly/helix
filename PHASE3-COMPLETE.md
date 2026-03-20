# Phase 3: Synapse API — COMPLETE
**Completed:** 2026-03-02
**Version:** v0.3.1
**Status:** ✅ DEPLOYED AND VERIFIED

## Summary
Phase 3 delivers the Synapse API — Helix's context injection and semantic search layer. The Synapse enables external systems (browser extension, MCP server, IDE plugins) to query Helix's knowledge base and receive contextually relevant code patterns for injection into LLM conversations.

## What Was Built

### Core Deliverables
1. **Embedding Service** (`services/embeddings.py`) — Local BGE-Large-EN-v1.5 (1024 dims) via fastembed ONNX runtime. Model cached in persistent volume, initialized at startup.

2. **ChromaDB Client Rewrite** (`services/chromadb.py`) — HTTP client that pre-computes embeddings before upsert/query. Fixes the fundamental issue: ChromaDB REST API requires `query_embeddings`, not `query_texts`.

3. **Synapse Routers** (`routers/synapse.py`):
   - `POST /api/v1/lifecycle/session/start` — Start tracked session
   - `POST /api/v1/lifecycle/session/end` — End session with summary
   - `GET /api/v1/lifecycle/sessions` — List sessions
   - `POST /api/v1/search/atoms` — Structural search (name, fingerprint)
   - `POST /api/v1/search/semantic` — Semantic vector search across all collections
   - `POST /api/v1/context/inject` — Full context assembly for LLM injection

4. **Synapse Service** (`services/synapse.py`) — Context assembly engine that:
   - Searches atoms, sessions, entities via semantic similarity
   - Enriches results with full epigenetic metadata
   - Ranks and deduplicates across collections
   - Assembles injection payload with token budget awareness

5. **CODE Intake Fix** (`services/worker.py`) — Worker now extracts fenced code blocks from conversation messages, enabling the full exchange → classify → scan → atom pipeline.

6. **JSON Parser Hardening** (`services/parser.py`) — 4-strategy JSON extraction handles all Haiku response formats: clean JSON, markdown fences, trailing explanatory text, and depth-tracked extraction.

### Infrastructure Changes
- `requirements.txt` — Added fastembed==0.4.1
- `Dockerfile` — ONNX system deps, model cache env, 120s health start period
- `config.py` — BGE-Large-EN-v1.5, 1024 dimensions
- `main.py` — Embedding service initialized before ChromaDB at startup
- ChromaDB: Cleaned stale collections (bge-m3, bge-small-en-v1.5), 3 active collections with bge-large-en-v1.5 suffix

## Test Results

### Semantic Search
```
Query: "authentication API key verification"
→ atom_43c022870570: verify_api_key (distance: 0.4274) ← correct top match
→ atom_f7451914cc51: rate_limit_middleware (distance: 1.0154)
→ atom_91e90d95f29b: connect (distance: 1.1061)
```

### Context Injection
```
POST /api/v1/context/inject {"query": "authentication middleware pattern"}
→ 200 OK with atoms including full epigenetic meta (structural, semantic, provenance)
```

### CODE Intake from Exchange
```
POST /api/v1/ingest with exchange containing fenced Python code in messages
→ Extracted 1 python code block (584 chars)
→ Created 2 atoms: verify_api_key, rate_limit_middleware
→ Full meta written: structural, semantic, provenance namespaces
```

## Architecture Decisions
| Decision | Rationale |
|----------|-----------|
| fastembed over sentence-transformers | ONNX runtime: ~150MB vs 2GB+ PyTorch. Same quality. |
| BGE-Large-EN-v1.5 over BGE-M3 | fastembed doesn't support M3. Large gives 1024 dims, excellent for English code. |
| httpx ChromaDB client | No heavy chromadb Python package. Direct REST API with pre-computed vectors. |
| Regex code fence extraction | Reliable for ```language\ncode\n``` blocks in conversation messages. |
| Future multilingual | Deploy Ollama on VPS1 as shared embedding service. Model-versioned collections enable seamless transition. |

## Phase Status Summary
| Phase | Status | Deliverables |
|-------|--------|-------------|
| Phase 1: Cortex Foundation | ✅ COMPLETE | 17 deliverables |
| Phase 2: Mitochondria Worker | ✅ COMPLETE | 11 deliverables |
| Phase 3: Synapse API | ✅ COMPLETE | 6 deliverables |
| Phase 4: Compression | 🔲 NOT STARTED | — |
| Phase 5: Editor | 🔲 NOT STARTED | — |
| Phase 6: Cockpit | 🔲 NOT STARTED | — |

## Next Phase
Phase 4: Compression — The token compression pipeline. Dictionary versioning, shorthand notation, compression logging, multi-layer compression metrics.
