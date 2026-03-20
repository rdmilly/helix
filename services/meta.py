"""Meta Service - Atomic Meta Operations

Handles atomic read/merge/write/rollback for epigenetic metadata.
All meta writes are transactional with event logging.
"""
import json
from services import pg_sync
import logging
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime
from services.database import get_db

logger = logging.getLogger(__name__)


class MetaService:
    """Atomic meta operations with event logging"""
    
    def __init__(self):
        self.db = get_db()
    
    def read_meta(self, table: str, record_id: str, namespace: Optional[str] = None) -> Dict[str, Any]:
        """
        Read meta from a record.
        Returns full meta dict if namespace=None, or specific namespace if provided.
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT meta FROM {table} WHERE id = ?", (record_id,))
            row = cursor.fetchone()
            
            if not row:
                raise ValueError(f"Record {record_id} not found in {table}")
            
            meta = pg_sync.dejson(row[0] or '{}')
            
            if namespace:
                return meta.get(namespace, {})
            return meta
    
    def write_meta(
        self,
        table: str,
        record_id: str,
        namespace: str,
        data: Dict[str, Any],
        written_by: str = "cortex"
    ) -> Dict[str, Any]:
        """
        Atomic meta write with event logging.
        
        Algorithm:
        1. Read current meta in transaction
        2. Deep-merge namespace data
        3. Write merged result
        4. Log event
        5. Commit transaction
        
        Returns the new full meta dict.
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Read current meta
                cursor.execute(f"SELECT meta FROM {table} WHERE id = ?", (record_id,))
                row = cursor.fetchone()
                
                if not row:
                    raise ValueError(f"Record {record_id} not found in {table}")
                
                current_meta = pg_sync.dejson(row[0] or '{}')
                old_namespace_value = current_meta.get(namespace, {})
                
                # Deep-merge namespace
                new_namespace_value = self._deep_merge(old_namespace_value, data)
                current_meta[namespace] = new_namespace_value
                
                # Write merged result
                cursor.execute(
                    f"UPDATE {table} SET meta = ? WHERE id = ?",
                    (json.dumps(current_meta), record_id)
                )
                
                # Log event
                event_id = f"evt_{uuid.uuid4().hex[:12]}"
                cursor.execute("""
                    INSERT INTO meta_events (id, target_table, target_id, namespace, action, old_value, new_value, written_by)
                    VALUES (?, ?, ?, ?, 'write', ?, ?, ?)
                """, (
                    event_id,
                    table,
                    record_id,
                    namespace,
                    json.dumps(old_namespace_value),
                    json.dumps(new_namespace_value),
                    written_by
                ))
                
                conn.commit()
                logger.info(f"Meta write: {table}.{record_id}.{namespace} by {written_by}")
                return current_meta
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Meta write failed: {e}")
                raise
    
    def merge_meta(
        self,
        table: str,
        record_id: str,
        namespace: str,
        updates: Dict[str, Any],
        written_by: str = "cortex"
    ) -> Dict[str, Any]:
        """
        Convenience method for partial namespace updates.
        Same as write_meta but explicitly named for clarity.
        """
        return self.write_meta(table, record_id, namespace, updates, written_by)
    
    def delete_namespace(
        self,
        table: str,
        record_id: str,
        namespace: str,
        written_by: str = "cortex"
    ) -> Dict[str, Any]:
        """
        Remove a namespace from meta (rare operation).
        Returns the new full meta dict.
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Read current meta
                cursor.execute(f"SELECT meta FROM {table} WHERE id = ?", (record_id,))
                row = cursor.fetchone()
                
                if not row:
                    raise ValueError(f"Record {record_id} not found in {table}")
                
                current_meta = pg_sync.dejson(row[0] or '{}')
                old_value = current_meta.pop(namespace, {})
                
                # Write updated meta
                cursor.execute(
                    f"UPDATE {table} SET meta = ? WHERE id = ?",
                    (json.dumps(current_meta), record_id)
                )
                
                # Log event
                event_id = f"evt_{uuid.uuid4().hex[:12]}"
                cursor.execute("""
                    INSERT INTO meta_events (id, target_table, target_id, namespace, action, old_value, new_value, written_by)
                    VALUES (?, ?, ?, ?, 'delete', ?, NULL, ?)
                """, (event_id, table, record_id, namespace, json.dumps(old_value), written_by))
                
                conn.commit()
                logger.info(f"Meta namespace deleted: {table}.{record_id}.{namespace}")
                return current_meta
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Meta namespace delete failed: {e}")
                raise
    
    def get_event_history(
        self,
        table: str,
        record_id: str,
        namespace: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get event history for a record.
        Optionally filter by namespace.
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            if namespace:
                cursor.execute("""
                    SELECT id, namespace, action, old_value, new_value, written_by, timestamp
                    FROM meta_events
                    WHERE target_table = ? AND target_id = ? AND namespace = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (table, record_id, namespace, limit))
            else:
                cursor.execute("""
                    SELECT id, namespace, action, old_value, new_value, written_by, timestamp
                    FROM meta_events
                    WHERE target_table = ? AND target_id = ?
                    ORDER BY timestamp DESC
                    LIMIT ?
                """, (table, record_id, limit))
            
            events = []
            for row in cursor.fetchall():
                events.append({
                    "id": row[0],
                    "namespace": row[1],
                    "action": row[2],
                    "old_value": pg_sync.dejson(row[3]) if row[3] else None,
                    "new_value": pg_sync.dejson(row[4]) if row[4] else None,
                    "written_by": row[5],
                    "timestamp": row[6]
                })
            
            return events
    
    def rollback_to_event(
        self,
        event_id: str,
        written_by: str = "cortex_rollback"
    ) -> bool:
        """
        Rollback a specific namespace to its state before an event.
        Creates a new event documenting the rollback.
        """
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Get the event
                cursor.execute("""
                    SELECT target_table, target_id, namespace, old_value
                    FROM meta_events
                    WHERE id = ?
                """, (event_id,))
                
                row = cursor.fetchone()
                if not row:
                    raise ValueError(f"Event {event_id} not found")
                
                table, record_id, namespace, old_value_str = row
                old_value = pg_sync.dejson(old_value_str) if old_value_str else {}
                
                # Read current meta
                cursor.execute(f"SELECT meta FROM {table} WHERE id = ?", (record_id,))
                meta_row = cursor.fetchone()
                if not meta_row:
                    raise ValueError(f"Record {record_id} not found in {table}")
                
                current_meta = pg_sync.dejson(meta_row[0] or '{}')
                current_namespace_value = current_meta.get(namespace, {})
                
                # Restore old value
                current_meta[namespace] = old_value
                
                # Write
                cursor.execute(
                    f"UPDATE {table} SET meta = ? WHERE id = ?",
                    (json.dumps(current_meta), record_id)
                )
                
                # Log rollback event
                rollback_event_id = f"evt_{uuid.uuid4().hex[:12]}"
                cursor.execute("""
                    INSERT INTO meta_events (id, target_table, target_id, namespace, action, old_value, new_value, written_by)
                    VALUES (?, ?, ?, ?, 'rollback', ?, ?, ?)
                """, (
                    rollback_event_id,
                    table,
                    record_id,
                    namespace,
                    json.dumps(current_namespace_value),
                    json.dumps(old_value),
                    written_by
                ))
                
                conn.commit()
                logger.info(f"Rolled back event {event_id} for {table}.{record_id}.{namespace}")
                return True
                
            except Exception as e:
                conn.rollback()
                logger.error(f"Rollback failed: {e}")
                raise
    
    def _deep_merge(self, base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep-merge two dictionaries.
        Updates takes precedence.
        """
        result = base.copy()
        
        for key, value in updates.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result


# Global service instance
meta_service = MetaService()


def get_meta_service() -> MetaService:
    """Get meta service instance"""
    return meta_service
