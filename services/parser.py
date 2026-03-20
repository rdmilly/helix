"""Parser utilities and ACTIONS/CHANGES content handler.

Two responsibilities:
1. JSON extraction from LLM responses (handles markdown fences, trailing text)
2. ParserService for ACTIONS and CHANGES content routing
"""
import json
import re
import logging
from typing import Any, Optional, Dict, List

logger = logging.getLogger(__name__)


# ============================================================
# JSON Extraction Utilities (used by HaikuService)
# ============================================================

def _find_json_object(text: str) -> Optional[Any]:
    """Find and parse the first complete JSON object or array in text.
    
    Uses depth-tracking to handle nested structures and ignores
    any trailing text after the closing bracket.
    """
    if not text:
        return None
    
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        start_idx = text.find(start_char)
        if start_idx == -1:
            continue
        
        depth = 0
        in_string = False
        escape_next = False
        
        for i in range(start_idx, len(text)):
            c = text[i]
            
            if escape_next:
                escape_next = False
                continue
            
            if c == '\\' and in_string:
                escape_next = True
                continue
            
            if c == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            if c == start_char:
                depth += 1
            elif c == end_char:
                depth -= 1
                if depth == 0:
                    candidate = text[start_idx:i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break
    
    return None


def extract_json(text: str) -> Optional[Any]:
    """Extract JSON from an LLM response, handling common formatting issues.
    
    Handles:
    - Clean JSON responses
    - JSON wrapped in ```json ... ``` fences
    - JSON with trailing explanatory text (Haiku's favorite)
    - JSON with leading preamble text
    - JSON inside fences WITH trailing text inside the fences
    
    Returns parsed JSON (dict, list, etc.) or None if no valid JSON found.
    """
    if not text or not text.strip():
        return None
    
    cleaned = text.strip()
    
    # Strategy 1: Direct parse (clean response)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass
    
    # Strategy 2: Extract from markdown fences, then find JSON within
    fence_match = re.search(r'```(?:json)?\s*\n?(.*?)```', cleaned, re.DOTALL)
    if fence_match:
        fence_content = fence_match.group(1).strip()
        # Try direct parse of fence content
        try:
            return json.loads(fence_content)
        except json.JSONDecodeError:
            pass
        # Fence content might have trailing text -- find JSON object within
        result = _find_json_object(fence_content)
        if result is not None:
            return result
    
    # Strategy 3: Find first complete JSON object or array anywhere in text
    result = _find_json_object(cleaned)
    if result is not None:
        return result
    
    logger.warning(f"No valid JSON found in response: {text[:150]}")
    return None


def extract_json_or_default(text: str, default: Any = None) -> Any:
    """Extract JSON from LLM response, returning default if not found."""
    result = extract_json(text)
    return result if result is not None else default


# ============================================================
# ParserService for ACTIONS and CHANGES content types
# ============================================================

class ParserService:
    """Parses ACTIONS and CHANGES content from intake payloads.
    
    ACTIONS: tool calls, file operations, shell commands, API calls
    CHANGES: git commits, config changes, infrastructure modifications
    """
    
    async def parse_actions(self, payload: Dict[str, Any], session_id: Optional[str] = None) -> Dict[str, Any]:
        """Parse ACTIONS content -- tool calls, file ops, commands.
        
        Extracts structured data about what actions were taken,
        which services/tools were involved, and any entities found.
        """
        content = payload.get("content", "")
        messages = payload.get("messages", [])
        
        # Collect all action-like content
        actions_found: List[Dict[str, Any]] = []
        entities_found: List[str] = []
        decisions_found: List[str] = []
        
        # Parse tool calls from messages
        for msg in messages[:50]:
            msg_content = msg.get("content", "")
            if isinstance(msg_content, str):
                # Detect tool call patterns
                if "tool_call" in msg_content or "ssh_execute" in msg_content:
                    actions_found.append({
                        "type": "tool_call",
                        "raw": msg_content[:500],
                    })
                
                # Detect file operations
                if any(kw in msg_content for kw in ["write_file", "create_file", "mkdir", "cp ", "mv "]):
                    actions_found.append({
                        "type": "file_operation",
                        "raw": msg_content[:500],
                    })
                
                # Detect docker operations
                if "docker" in msg_content.lower():
                    actions_found.append({
                        "type": "docker_operation",
                        "raw": msg_content[:500],
                    })
                
                # Extract service names
                for pattern in [r'(\w+)\.millyweb\.com', r'port\s+(\d{4,5})', r'/opt/projects/(\w[\w-]*)']:
                    for match in re.finditer(pattern, msg_content):
                        entities_found.append(match.group(0))
        
        # Parse direct content
        if content:
            for pattern in [r'(\w+)\.millyweb\.com', r'/opt/projects/(\w[\w-]*)']:
                for match in re.finditer(pattern, content):
                    entities_found.append(match.group(0))
        
        return {
            "actions": actions_found,
            "action_count": len(actions_found),
            "entities_found": list(set(entities_found)),
            "decisions_found": decisions_found,
        }
    
    async def parse_changes(self, payload: Dict[str, Any], session_id: Optional[str] = None) -> Dict[str, Any]:
        """Parse CHANGES content -- git commits, config changes, infra modifications.
        
        Extracts structured data about what changed, which files/services
        were modified, and any entities found.
        """
        content = payload.get("content", "")
        messages = payload.get("messages", [])
        
        changes_found: List[Dict[str, Any]] = []
        entities_found: List[str] = []
        files_changed: List[str] = []
        
        # Parse commit-like patterns
        all_text = content
        for msg in messages[:50]:
            msg_content = msg.get("content", "")
            if isinstance(msg_content, str):
                all_text += "\n" + msg_content
        
        # Git patterns
        for match in re.finditer(r'(?:commit|merged?|pushed?)\s+([a-f0-9]{7,40})', all_text, re.IGNORECASE):
            changes_found.append({
                "type": "git_commit",
                "hash": match.group(1),
            })
        
        # File change patterns
        for match in re.finditer(r'(?:modified|created|deleted|changed)\s+(\S+\.\w+)', all_text, re.IGNORECASE):
            files_changed.append(match.group(1))
        
        # Config change patterns
        for match in re.finditer(r'(?:docker-compose|\.env|\.yaml|\.yml|\.toml|\.conf)', all_text, re.IGNORECASE):
            changes_found.append({
                "type": "config_change",
                "file": match.group(0),
            })
        
        # Extract entity references
        for pattern in [r'(\w+)\.millyweb\.com', r'/opt/projects/(\w[\w-]*)']:
            for match in re.finditer(pattern, all_text):
                entities_found.append(match.group(0))
        
        return {
            "changes": changes_found,
            "change_count": len(changes_found),
            "files_changed": list(set(files_changed)),
            "entities_found": list(set(entities_found)),
        }


# Global service instance
_parser_service = ParserService()


def get_parser_service() -> ParserService:
    """Get parser service instance."""
    return _parser_service
