"""Helix Cortex Configuration

Central configuration for Helix.
All secrets from environment variables (Infisical injection).
"""
import os
from pathlib import Path

# === PATHS ===
BASE_DIR = Path("/app")
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "cortex.db"

# === API ===
PORT = int(os.getenv("HELIX_PORT", "9050"))
HOST = os.getenv("HELIX_HOST", "0.0.0.0")
ALLOWED_ORIGINS = os.getenv("HELIX_ALLOWED_ORIGINS", "*").split(",")

# === DATABASE ===
MAX_QUEUE_DEPTH = int(os.getenv("HELIX_MAX_QUEUE_DEPTH", "1000"))
MAX_RETRY_ATTEMPTS = int(os.getenv("HELIX_MAX_RETRY_ATTEMPTS", "3"))

# === CHROMADB ===
CHROMADB_HOST = os.getenv("CHROMADB_HOST", "localhost")
CHROMADB_PORT = int(os.getenv("CHROMADB_PORT", "8000"))
CURRENT_EMBEDDING_MODEL = "bge-large-en-v1.5"  # Best 1024-dim model in fastembed
EMBEDDING_DIMENSIONS = 1024

# === HAIKU API ===
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
# HAIKU_MODEL set below via OpenRouter block
HAIKU_MAX_RETRIES = 3
HAIKU_TIMEOUT = 30

# === MINIO ===
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio.millyweb.com")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "helix")

# === ALGORITHM VERSIONS ===
CURRENT_FP_VERSION = "v1"  # Fingerprint algorithm version
CURRENT_DICTIONARY_VERSION = "v1"  # Bootstrap version

# === CIRCUIT BREAKER THRESHOLDS ===
CIRCUIT_BREAKER_THRESHOLD = 5  # Failures before opening
CIRCUIT_BREAKER_TIMEOUT = 60  # Seconds before retry

# === SECURITY ===
CREDENTIAL_ENCRYPTION_KEY = os.getenv("HELIX_ENCRYPTION_KEY")  # From Infisical
CREDENTIAL_PATTERNS = [
    r"sk-[a-zA-Z0-9]{48}",  # OpenAI API keys
    r"anthropic_[a-zA-Z0-9_]{40,}",  # Anthropic keys
    r"ghp_[a-zA-Z0-9]{36}",  # GitHub tokens
    r"[a-zA-Z0-9]{32}\.[a-zA-Z0-9]{6}\.[a-zA-Z0-9_-]{27}",  # JWT
]

# Ensure data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)

# === CONVERSATION RAG (Ported from Memory v0.5) ===
CHUNK_TARGET_TOKENS = int(os.getenv("CHUNK_TARGET_TOKENS", "300"))   # ~1200 chars
CHUNK_MAX_TOKENS = int(os.getenv("CHUNK_MAX_TOKENS", "500"))         # ~2000 chars
CHUNK_OVERLAP_TOKENS = int(os.getenv("CHUNK_OVERLAP_TOKENS", "50"))  # ~200 chars

RAG_RESULTS_DEFAULT = 5
RAG_RESULTS_MAX = 20
RAG_VECTOR_WEIGHT = 0.6    # RRF weight for vector similarity
RAG_BM25_WEIGHT = 0.4      # RRF weight for keyword match
RAG_RRF_K = 60             # RRF constant (standard)

FTS_DB_PATH = DATA_DIR / "conversations_fts.db"

TEMPORAL_DECAY_ENABLED = True
TEMPORAL_DECAY_HALF_LIFE_DAYS = 30  # Score halves every 30 days
TEMPORAL_DECAY_MIN_FACTOR = 0.3     # Floor — never decay below 30%

# === POSTGRESQL (Phase 4 migration) ===
import os as _os
POSTGRES_DSN = _os.getenv(
    "POSTGRES_DSN",
    "host=helix-postgres user=helix password=934d69eb7ce6a90710643e93efe36fcc dbname=helix"
)

# === OPENROUTER (LLM fallback, OpenAI-compat) ===
OPENROUTER_API_KEY = _os.getenv("OPENROUTER_API_KEY", "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")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
# Use haiku via OpenRouter — same model, OpenAI-compat endpoint
HAIKU_MODEL = _os.getenv("HAIKU_MODEL", "anthropic/claude-haiku-4-5")

# === LLMPort provider selection ===
# Set LLM_PROVIDER to: openrouter | anthropic | ollama
LLM_PROVIDER = _os.getenv("LLM_PROVIDER", "openrouter")
LLM_MODEL    = _os.getenv("LLM_MODEL", "")  # empty = provider default
