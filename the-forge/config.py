"""The Forge — Configuration."""
import os
from pathlib import Path

# Server
PORT = int(os.environ.get("PORT", 9095))
DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

# Storage
DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
DB_PATH = DATA_DIR / "forge.db"
WORKSPACE_ROOT = Path(os.environ.get("WORKSPACE_ROOT", "/workspace"))

# Object storage (S3-compatible — Garage by default, MinIO/S3 optional)
# Rename kept as MINIO_* for backwards compat with existing minio SDK client
MINIO_ENDPOINT = os.environ.get("STORAGE_ENDPOINT", os.environ.get("MINIO_ENDPOINT", "helix-garage:3900"))
MINIO_ACCESS_KEY = os.environ.get("STORAGE_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY", ""))
MINIO_SECRET_KEY = os.environ.get("STORAGE_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY", ""))
MINIO_BUCKET = os.environ.get("STORAGE_BUCKET", os.environ.get("MINIO_BUCKET", "workspace"))
STORAGE_REGION = os.environ.get("STORAGE_REGION", os.environ.get("MINIO_REGION", "garage"))
MINIO_SECURE = os.environ.get("STORAGE_SECURE", os.environ.get("MINIO_SECURE", "false")).lower() == "true"

# PrintBlocks catalog path
PRINTBLOCKS_DIR = Path(os.environ.get("PRINTBLOCKS_DIR", "/printblocks"))

# Scanning
SCANNABLE_EXTENSIONS = {
    '.py', '.js', '.ts', '.jsx', '.tsx', '.html', '.css',
    '.sh', '.bash', '.yaml', '.yml', '.json', '.toml',
    '.sql', '.md', '.dockerfile', '.cfg', '.conf', '.ini',
}
HELIX_URL = os.environ.get("HELIX_URL", "http://helix-cortex:9050")
