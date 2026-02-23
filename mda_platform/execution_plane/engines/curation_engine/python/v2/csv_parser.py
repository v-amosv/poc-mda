# lib/engines/curation/v2/csv_parser.py
"""
V2 CSV Parser Component

V2 Enhancement: Converts all string values to LOWERCASE for normalization.
Reads parsed data from the 'raw' zone JSON envelope (same as V1).
"""
import json
import os
from pathlib import Path

# Resolve project root for relative paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent


def _normalize_to_lowercase(value):
    """
    V2 Enhancement: Convert string values to lowercase.
    Numbers remain unchanged.
    """
    if isinstance(value, str):
        return value.lower()
    return value


def run(ctx: dict, params: dict) -> str:
    """
    Read parsed data from raw JSON envelope and normalize strings to lowercase.
    
    V2 Enhancement: All string values are converted to lowercase.
    
    Args:
        ctx: Execution context containing:
            - utid: Unified Trace ID
            - ingested_file: Path to the ingested JSON file (set by ingest step)
            - raw_source_utid: (REPLAY MODE) Use Raw from this UTID instead
        params: Component parameters (kept for interface compatibility)
    
    Returns:
        Success message with row count
    """
    utid = ctx["utid"]
    
    # REPLAY MODE: Use historical Raw file from source UTID
    lookup_utid = ctx.get("raw_source_utid", utid)
    replay_mode = ctx.get("replay_mode", False)
    
    if replay_mode:
        print(f"      ⏮️  REPLAY: Looking for Raw file from {lookup_utid[:12]}...")
    
    # Get the ingested file path from context (set by ingest step)
    ingested_file = ctx.get("ingested_file")
    
    if not ingested_file:
        # Fallback: scan raw directory for UTID-prefixed files
        # In replay mode, this finds the historical file
        raw_base = _PROJECT_ROOT / "mda_platform" / "storage_plane" / "raw"
        
        # Search all raw subdirectories for the UTID-prefixed file
        for raw_dir in raw_base.iterdir():
            if raw_dir.is_dir():
                for f in raw_dir.iterdir():
                    if lookup_utid in f.name and f.suffix == ".json":
                        ingested_file = str(f)
                        break
            if ingested_file:
                break
    
    if not ingested_file:
        if replay_mode:
            raise FileNotFoundError(
                f"No Raw file found for source UTID {lookup_utid}. "
                "The historical Raw data may have been deleted."
            )
        else:
            raise FileNotFoundError(
                f"No ingested file found for UTID {utid}. "
                "Ensure ingestion step ran successfully."
            )
    
    if replay_mode:
        print(f"      ⏮️  REPLAY: Found Raw file: {os.path.basename(ingested_file)}")
    
    # Make path absolute if relative
    if not os.path.isabs(ingested_file):
        ingested_file = str(_PROJECT_ROOT / ingested_file)
    
    # Read JSON envelope
    with open(ingested_file, "r") as f:
        raw_envelope = json.load(f)
    
    # Extract data and metadata
    metadata = raw_envelope.get("metadata", {})
    parsed_data = raw_envelope.get("data", [])
    
    # Store doc_id in context for downstream traceability
    if "doc_id" in metadata:
        ctx["doc_id"] = metadata["doc_id"]
    
    # V2 ENHANCEMENT: Convert all string values to lowercase
    normalized_data = []
    for row in parsed_data:
        normalized_row = {
            key: _normalize_to_lowercase(value)
            for key, value in row.items()
        }
        normalized_data.append(normalized_row)
    
    # Store parsed data in context for downstream components
    ctx["parsed_data"] = normalized_data
    
    row_count = len(normalized_data)
    col_count = len(normalized_data[0].keys()) if normalized_data else 0
    filename = os.path.basename(ingested_file)
    
    mode_tag = " [REPLAY]" if replay_mode else ""
    return f"PARSE_SUCCESS{mode_tag}: {row_count} rows x {col_count} cols from {filename} (V2 parser - LOWERCASE)"


# MDA Component Metadata
# Required for RuntimeResolver validation
run.__mda_component__ = {
    "version": "2.0.0",
    "interface": "mda.interfaces.parse.v1"
}
