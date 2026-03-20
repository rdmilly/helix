# Helix Phase 2: Mitochondria — Complete ✅

**Date:** March 2, 2026  
**Version:** 0.2.1  
**Status:** ALL 11 PHASE 2 DELIVERABLES COMPLETE + PARSER FIX

## What Was Built (Phase 2)

### Background Worker (Mitochondria)
- **services/worker.py** — Polls SQLite queue every 5s, batch size 10, graceful shutdown
- Content routing: CODE → Scanner, TEXT → Haiku, ACTIONS/CHANGES → Parser
- Epigenetic meta enrichment on every processed item

### Haiku API Integration (Real Calls)
- **services/haiku.py** — Anthropic API client with circuit breaker, retry logic
- Summarization, classification (CODE/ACTIONS/TEXT/CHANGES), entity extraction, decision extraction
- Heuristic fallback when circuit breaker is open

### AST Scanner
- **services/scanner.py** — Python AST extraction with dual fingerprinting
- Structural fingerprint (signature-based) + behavioral fingerprint (body-hash)
- Atom creation with meta enrichment (structural, semantic, provenance namespaces)

### ChromaDB Integration
- **services/chromadb.py** — Lightweight httpx-based REST client (avoids heavy chromadb package)
- Model-versioned collections: helix_atoms_bge-m3, helix_sessions_bge-m3, helix_entities_bge-m3
- Circuit breaker for resilience

### Parser Service (v0.2.1 — enhanced)
- **services/parser.py** — Dual purpose: JSON extraction utils + ACTIONS/CHANGES parser
- Robust `extract_json()` handles markdown fences, trailing text, preamble
- Tool call, file operation, docker operation, and config change detection

## Phase 2 Deliverables Checklist

✅ 1. Background worker loop (5s poll, batch 10)
✅ 2. Content classification via Haiku API
✅ 3. CODE routing → Scanner (AST extraction + fingerprinting)
✅ 4. TEXT routing → Haiku (summarization + entity + decision extraction)
✅ 5. ACTIONS routing → Parser (tool calls, file ops)
✅ 6. CHANGES routing → Parser (commits, config changes)
✅ 7. ChromaDB document storage (model-versioned collections)
✅ 8. Entity extraction (people, projects, services, technologies)
✅ 9. Decision extraction (architecture, deployment, config, process, design)
✅ 10. Graceful worker shutdown
✅ 11. Worker stats endpoint (/api/v1/worker/stats)

## v0.2.1 Fix (March 2, 2026)
- **Haiku JSON parsing** — Robust `extract_json()` utility handles:
  - Clean JSON
  - JSON in markdown fences (```json ... ```)
  - JSON followed by trailing explanatory text
  - JSON preceded by preamble text
- Previously: decision extraction failed when Haiku returned `\`\`\`json\n[]\n\`\`\`\n\nNo decisions...`
- Now: all three extraction methods (classify, entities, decisions) use the robust parser

## Testing Results

### TEXT Pipeline
- Intake → Queue → Worker → Haiku (classify + entities + decisions) → ChromaDB → Complete
- No parsing errors, all meta writes successful

### CODE Pipeline
- Intake → Queue → Worker → Haiku (classify as CODE) → Scanner (AST extract) → ChromaDB → Complete
- 3 atoms extracted from test file: `__init__`, `connect`, `process_item`
- Structural, semantic, and provenance meta written for each atom

### Production Stats
- 8 total items processed, 0 errors
- 6 sessions created
- 3 atoms in DNA library
- 3 ChromaDB collections active

## Architecture Decisions (Phase 2)

1. **httpx over chromadb package** — chromadb Python package pulls numpy/onnxruntime, too heavy for container. httpx REST client is ~150 lines and does everything needed.
2. **Heuristic fallback** — When Haiku circuit breaker is open, keyword-based classification keeps the pipeline running.
3. **Atomic meta enrichment** — Each processing step writes to specific meta namespaces. Worker doesn't batch meta writes.
4. **Robust JSON extraction** — Three-strategy parser: direct parse → fence extraction → bracket matching. Handles all known LLM response formats.

## Files Created/Modified (Phase 2)

```
/opt/projects/helix/
├── services/
│   ├── worker.py      (NEW - 310 lines)
│   ├── scanner.py     (IMPLEMENTED - was stub)
│   ├── haiku.py       (IMPLEMENTED - was stub, v0.2.1 fix)
│   ├── chromadb.py    (IMPLEMENTED - was stub)
│   └── parser.py      (NEW - JSON utils + ACTIONS/CHANGES parser)
├── main.py            (UPDATED - worker integration, v0.2.1)
└── migrate.py         (NEW - schema migration helper)
```

## Next: Phase 3 — Synapse API

Phase 3 adds the context injection layer:
- Session lifecycle management (open/close/query sessions)
- Context search endpoints (semantic search, atom lookup)
- Context injection for new sessions (pull relevant atoms/decisions)
- Search across atoms, sessions, entities, decisions
