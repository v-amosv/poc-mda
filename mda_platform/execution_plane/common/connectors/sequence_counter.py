# platform/execution_plane/lib/connectors/sequence_counter.py
"""
Sequence Counter for UTID-based file naming.

Provides sequential numbering for files across stores:
- raw: raw-0001, raw-0002, ...
- fact: fact-0001, fact-0002, ...
- evidence: evidence-0001, evidence-0002, ...

Each store maintains its own sequence counter stored in a .seq file.
"""
import json
from pathlib import Path
from typing import Dict

# Storage plane location
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"

# Sequence file locations
_SEQ_FILES = {
    # Data stores
    "raw": _STORAGE_PLANE / "raw" / ".seq",
    "fact": _STORAGE_PLANE / "fact_store" / ".seq",
    "semantic_store": _STORAGE_PLANE / "semantic_store" / ".seq",
    "retrieval_store": _STORAGE_PLANE / "retrieval_store" / ".seq",
    # Evidence store
    "evidence": _STORAGE_PLANE / "evidence_store" / ".seq",
    "curation": _STORAGE_PLANE / "evidence_store" / ".seq_curation",
    "semantic": _STORAGE_PLANE / "evidence_store" / ".seq_semantic",
    "retrieval": _STORAGE_PLANE / "evidence_store" / ".seq_retrieval",
}


def _load_seq(store: str) -> int:
    """Load current sequence number for a store."""
    seq_file = _SEQ_FILES.get(store)
    if not seq_file or not seq_file.exists():
        return 0
    
    try:
        with open(seq_file, "r") as f:
            data = json.load(f)
        # Handle both old format (bare int) and new format (dict)
        if isinstance(data, int):
            return data
        return data.get("seq", 0)
    except (json.JSONDecodeError, IOError):
        return 0


def _save_seq(store: str, seq: int) -> None:
    """Save sequence number for a store."""
    seq_file = _SEQ_FILES.get(store)
    if not seq_file:
        return
    
    seq_file.parent.mkdir(parents=True, exist_ok=True)
    with open(seq_file, "w") as f:
        json.dump({"seq": seq}, f)


def next_seq(store: str) -> int:
    """
    Get next sequence number for a store.
    
    Args:
        store: One of 'raw', 'fact', 'evidence'
        
    Returns:
        Next sequence number (1-based)
    """
    current = _load_seq(store)
    next_num = current + 1
    _save_seq(store, next_num)
    return next_num


def format_filename(store: str, utid: str, suffix: str = "") -> str:
    """
    Generate filename with sequence and UTID.
    
    Args:
        store: One of 'raw', 'fact', 'evidence'
        utid: The UTID (with or without 'utid-' prefix)
        suffix: Optional suffix (e.g., '_bls_employment_stats_v1.0.0')
        
    Returns:
        Formatted filename like 'raw-0001-utid-abc123...'
    """
    # Strip 'utid-' prefix if present to avoid duplication
    utid_clean = utid.replace("utid-", "")
    
    seq = next_seq(store)
    return f"{store}-{seq:04d}-utid-{utid_clean}{suffix}"


def reset_all_sequences() -> None:
    """Reset all sequence counters to 0."""
    for store in _SEQ_FILES:
        seq_file = _SEQ_FILES[store]
        if seq_file.exists():
            seq_file.unlink()


def get_current_seq(store: str) -> int:
    """Get current sequence number without incrementing."""
    return _load_seq(store)
