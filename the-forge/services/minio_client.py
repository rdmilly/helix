"""The Forge — S3-compatible object storage client.
Works with Garage (default), MinIO, or any S3-compatible backend.
Note: S3 object versioning NOT used — Forge's SQLite tracks versions.
"""

import io
import logging
from minio import Minio
from minio.error import S3Error
from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, MINIO_SECURE, STORAGE_REGION

logger = logging.getLogger("forge.storage")

client: Minio | None = None


def init_minio() -> bool:
    """Initialize S3 client and ensure bucket exists."""
    global client
    if not MINIO_ACCESS_KEY:
        logger.warning("Storage credentials not set — filesystem-only mode")
        return False
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            region=STORAGE_REGION,
            secure=MINIO_SECURE
        )
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created bucket: {MINIO_BUCKET}")
        logger.info(f"Storage connected: {MINIO_ENDPOINT}/{MINIO_BUCKET}")
        return True
    except Exception as e:
        logger.error(f"Storage init failed: {e}")
        client = None
        return False


def put_file(path: str, content: str) -> str | None:
    """Store file content. Returns object name (version tracked in SQLite)."""
    if not client:
        return None
    try:
        data = content.encode('utf-8')
        client.put_object(
            MINIO_BUCKET, path,
            io.BytesIO(data), len(data),
            content_type='text/plain'
        )
        return path
    except S3Error as e:
        logger.error(f"Storage put error for {path}: {e}")
        return None


def get_file(path: str, version_id: str | None = None) -> str | None:
    """Retrieve file content from storage."""
    if not client:
        return None
    try:
        response = client.get_object(MINIO_BUCKET, path)
        return response.read().decode('utf-8')
    except S3Error as e:
        if e.code == 'NoSuchKey':
            return None
        logger.error(f"Storage get error for {path}: {e}")
        return None


def delete_file(path: str) -> bool:
    """Delete file from storage."""
    if not client:
        return False
    try:
        client.remove_object(MINIO_BUCKET, path)
        return True
    except S3Error as e:
        logger.error(f"Storage delete error for {path}: {e}")
        return False


def list_files(prefix: str = "") -> list[dict]:
    """List files in storage bucket."""
    if not client:
        return []
    try:
        objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)
        return [
            {"path": obj.object_name, "size": obj.size, "modified": str(obj.last_modified)}
            for obj in objects
        ]
    except S3Error as e:
        logger.error(f"Storage list error: {e}")
        return []
