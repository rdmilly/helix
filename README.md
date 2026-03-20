# Helix Cortex - Phase 1: Foundation

**Version:** 0.1.0  
**Status:** Phase 1 Complete  
**Architecture:** Epigenetic Data Architecture + Algorithm Versioning

## What is Helix?

Helix (Double Helix) is an AI-native self-learning system that captures, analyzes, and evolves code patterns over time. This is Phase 1: Cortex Foundation - the backend intelligence center with full epigenetic data architecture.

### System Components (Helix Naming Scheme)

- **Helix** - The whole system
- **Membrane** - Browser extension (Phase 4)
- **Cortex** - Backend (this project)
- **DNA** - Pattern library (atoms, molecules, organisms)
- **Editor** - Assembler that creates Expressions (Phase 5)
- **Enzymes** - Compression engine (Phase 4)
- **Nervous System** - Cockpit dashboard (Phase 6)
- **Flagella** - MCP server (Phase 7)
- **Evolution** - Self-learning feedback loop

## Phase 1 Deliverables ✓

1. ✓ Epigenetic schema - ALL tables with meta columns
2. ✓ Algorithm versioning - fp_version, dictionary_versions, tokenizer tracking
3. ✓ Registry-based intake - POST /api/v1/ingest
4. ✓ Content type classifier (CODE/ACTIONS/TEXT/CHANGES)
5. ✓ Atomic meta service - transactional meta operations
6. ✓ Namespace registry - extensible schema management
7. ✓ Dictionary service - append-only with immutable symbols
8. ✓ Idempotent intake - content hash deduplication
9. ✓ API versioning - all endpoints under /api/v1/
10. ✓ Graceful degradation - circuit breakers for ChromaDB, Haiku, MinIO
11. ✓ Health probes - /health + /ready
12. ✓ Rate limiting - queue max depth + 429 backpressure
13. ✓ Credential encryption - detect-before-store
14. ✓ SQLite-backed queue - persistent with retry logic
15. ✓ ChromaDB client init - model-versioned collections
16. ✓ Bootstrap data - seed registries
17. ✓ Stub routers - all return 501 Not Implemented

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.12+ (for local development)
- Environment variables (see .env.example)

### Deploy

```bash
cd /opt/projects/helix
docker-compose up -d --build
```

### Verify

```bash
# Health check
curl http://localhost:9050/health

# Readiness check
curl http://localhost:9050/ready

# API docs
open http://localhost:9050/docs
```

## API Endpoints

### Active Endpoints (Phase 1)

- `POST /api/v1/ingest` - Universal intake endpoint
- `GET /api/v1/queue/status/{queue_id}` - Check queue item status
- `GET /api/v1/queue/stats` - Queue statistics
- `GET /health` - Liveness probe
- `GET /ready` - Readiness probe

### Phase 2+ Endpoints (501 Not Implemented)

- Search endpoints (Phase 3)
- Editor endpoints (Phase 5)
- Compression endpoints (Phase 4)
- Cockpit endpoints (Phase 6)
- Lifecycle endpoints (Phase 3)

## Architecture

### Epigenetic Data Architecture

Every table has `meta TEXT DEFAULT '{}'`. Metadata grows through namespace enrichment, never through schema migration.

**Core identity** (SQL columns) is fixed. **Metadata namespaces** (JSON in meta) accumulate over time from different subsystems.

Three infrastructure tables:
- `meta_namespaces` - Schema registry
- `type_registry` - Extensible classification
- `meta_events` - Audit trail

### Algorithm Versioning (5 Points)

1. **Fingerprints** - `fp_version` column on atoms
2. **Dictionary** - Append-only, immutable symbols
3. **Embeddings** - ChromaDB collections tagged with model version
4. **Tokenizer** - `tokenizer` column on compression_log
5. **Templates** - `template_format` in meta.structural

## Project Structure

```
helix/
├── main.py                  # FastAPI entry point
├── config.py                # Configuration
├── models/
│   ├── meta.py             # Epigenetic metadata models
│   └── schemas.py          # Intake payload models
├── services/
│   ├── database.py         # SQLite with full epigenetic schema
│   ├── meta.py             # Atomic meta operations
│   ├── registry.py         # Type & namespace registries
│   ├── dictionary.py       # Append-only dictionary
│   ├── scanner.py          # AST extraction (stub)
│   ├── chromadb.py         # Vector storage (stub)
│   └── haiku.py            # LLM client (stub)
├── routers/
│   ├── intake.py           # Active intake router
│   └── stubs.py            # Phase 2+ stub routers
├── data/
│   └── cortex.db           # SQLite database
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Database Schema

See `services/database.py` for full schema. Key tables:

**DNA Tables:**
- atoms, molecules, organisms, conventions

**Intelligence Tables:**
- sessions, decisions, entities, anomalies, nudges

**Infrastructure Tables:**
- queue, intake_hashes, compression_log
- meta_namespaces, type_registry, meta_events
- dictionary_versions

All tables have `meta TEXT DEFAULT '{}'` for epigenetic enrichment.

## Development

### Local Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

### Environment Variables

See `.env.example` for required variables.

## Next Phases

- **Phase 2:** Mitochondria (processing pipeline, Haiku calls, DNA scanner)
- **Phase 3:** Synapse API (context injection, session lifecycle)
- **Phase 4:** Membrane Integration (browser extension, Enzymes proxy)
- **Phase 5:** Editor & Expressions (template assembly)
- **Phase 6:** Nervous System UI (Organism View, cockpit dashboard)
- **Phase 7:** Flagella (MCP server for non-browser capture)

## References

- `specs/helix-naming-scheme.md` - Naming convention
- `specs/self-expanding-design.md` - Epigenetic Data Architecture
- `specs/millyforge-unified-v3.md` - Full architecture
- `/opt/projects/helix-tests/` - Test suite

## License

Proprietary - MW Development
