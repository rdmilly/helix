"""Helix Cortex Configuration

Central configuration for Helix Phase 4.
All secrets from environment variables (Infisical injection).
"""
import os
from pathlib import Path

# === PATHS ===
BASE_DIR = Path("/app")
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "cortex.db"
GRAMMAR_DIR = DATA_DIR / "grammars"  # Tree-sitter WASM grammars (auto-downloaded)

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
CURRENT_EMBEDDING_MODEL = "bge-small-en-v1.5"  # fastembed ONNX model (384 dims)
EMBEDDING_DIMENSIONS = 384

# === HAIKU API ===
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
HAIKU_MODEL = "claude-haiku-4-5-20251001"
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

# === SCANNER (Self-Expanding Three-Tier) ===
HEURISTIC_GENERATION_THRESHOLD = int(os.getenv("HELIX_HEURISTIC_THRESHOLD", "30"))
# Number of LLM analyses needed before auto-generating heuristic rules

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

# Ensure data directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
GRAMMAR_DIR.mkdir(parents=True, exist_ok=True)
