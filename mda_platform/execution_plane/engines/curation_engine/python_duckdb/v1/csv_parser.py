# platform/execution_plane/engines/curation_engine/python_duckdb/v1/csv_parser.py
"""
V1 DuckDB CSV Parser Component

Reads parsed data from the 'raw' zone JSON envelope.
DUCKDB ENGINE BEHAVIOR: Uses DuckDB in-memory database for SQL-based processing.
                        Preserves original string casing (no transformation).

This engine uses a real DuckDB connection to process data, demonstrating
that the MDA can swap between different execution engines seamlessly.
"""
import json
import os
import sys
from pathlib import Path

# Handle potential import issues
try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

# Resolve project root for relative paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent


def _process_with_duckdb(data: list) -> list:
    """
    Process data through DuckDB in-memory database.
    This demonstrates real DuckDB SQL processing within the MDA engine.
    Data passes through unchanged (preserving original case).
    """
    if not data:
        return data
    
    if not _DUCKDB_AVAILABLE:
        print("      ⚠️  DuckDB not available, passing data through unchanged")
        return data
    
    try:
        # Create in-memory DuckDB connection
        con = duckdb.connect(":memory:")
        
        # Convert list of dicts to DuckDB table using Python relation
        import pandas as pd
        df = pd.DataFrame(data)
        con.register("input_data", df)
        
        # Query all data (pass-through, preserving original values)
        result = con.execute("SELECT * FROM input_data").fetchdf()
        
        # Convert back to list of dicts
        output = result.to_dict(orient="records")
        
        con.close()
        return output
    except Exception as e:
        print(f"      ⚠️  DuckDB processing failed: {str(e)[:50]}, passing data through unchanged")
        return data


def run(ctx: dict, params: dict) -> str:
    """
    Read parsed data from raw JSON envelope.
    Processes data through DuckDB (preserves original casing).
    
    Args:
        ctx: Execution context containing:
            - utid: Unified Trace ID
            - ingested_file: Path to the ingested JSON file
            - raw_source_utid: (REPLAY MODE) Use Raw from this UTID instead
        params: Component parameters
    
    Returns:
        Success message with row count
    """
    utid = ctx["utid"]
    
    # REPLAY MODE: Use historical Raw file from source UTID
    lookup_utid = ctx.get("raw_source_utid", utid)
    replay_mode = ctx.get("replay_mode", False)
    
    if replay_mode:
        print(f"      ⏮️  REPLAY: Looking for Raw file from {lookup_utid[:12]}...")
    
    # Get the ingested file path from context
    ingested_file = ctx.get("ingested_file")
    
    if not ingested_file:
        # Fallback: scan raw directory for UTID-prefixed files
        raw_base = _PROJECT_ROOT / "mda_platform" / "storage_plane" / "raw"
        
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
    
    # DUCKDB ENGINE: Process through DuckDB (preserves original case)
    parsed_data = _process_with_duckdb(parsed_data)
    
    # Store parsed data in context for downstream components
    ctx["parsed_data"] = parsed_data
    
    row_count = len(parsed_data)
    col_count = len(parsed_data[0].keys()) if parsed_data else 0
    filename = os.path.basename(ingested_file)
    
    mode_tag = " [REPLAY]" if replay_mode else ""
    return f"PARSE_SUCCESS{mode_tag}: {row_count} rows x {col_count} cols from {filename} (🦆 DuckDB - original case)"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.parse.v1"
}
