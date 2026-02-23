# platform/execution_plane/engines/curation_engine/python_duckdb/v1/ingest_default.py
"""
V1 Default Ingestion Component (DuckDB Engine)

Moves data from 'wild' (external source) to 'raw' (landing zone).
Uses DuckDB's native read_csv() for parsing, demonstrating engine-native tools.

DuckDB Operations Used:
- duckdb.read_csv() - Native CSV parsing with auto-detection
- .fetchdf() / .fetchall() - Convert results to Python structures
"""
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# DuckDB import
try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

# Resolve project root for relative paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"

sys.path.insert(0, str(_PROJECT_ROOT))
from mda_platform.execution_plane.common.connectors.sequence_counter import format_filename
from mda_platform.control_plane.registry.parser_registry import get_parser


def _resolve_storage_path(path: str) -> str:
    """Resolve storage paths to the new structure."""
    if path.startswith("wild/"):
        return str(_STORAGE_PLANE / path)
    if path.startswith("raw/"):
        return str(_STORAGE_PLANE / path)
    if not os.path.isabs(path):
        return str(_PROJECT_ROOT / path)
    return path


def _parse_csv_duckdb(file_path: str) -> list:
    """
    Parse CSV using DuckDB's native read_csv() function.
    DuckDB automatically detects types and handles CSV parsing.
    """
    con = duckdb.connect(":memory:")
    
    # Use DuckDB's native CSV reader with auto-detection
    result = con.execute(f"""
        SELECT * FROM read_csv('{file_path}', 
            header=true, 
            auto_detect=true
        )
    """).fetchdf()
    
    con.close()
    
    # Convert to list of dicts
    return result.to_dict(orient="records")


def _parse_csv_fallback(file_path: str) -> list:
    """Fallback CSV parser using standard Python."""
    import csv
    with open(file_path, mode="r") as f:
        reader = csv.reader(f, delimiter=",")
        rows = list(reader)
    
    if not rows:
        return []
    
    headers = rows[0]
    data = []
    for row in rows[1:]:
        record = {}
        for i, header in enumerate(headers):
            if i < len(row):
                value = row[i].strip()
                try:
                    if '.' in value:
                        record[header] = float(value)
                    else:
                        record[header] = int(value)
                except ValueError:
                    record[header] = value if value else None
            else:
                record[header] = None
        data.append(record)
    return data


def _compute_hash(file_path: str) -> str:
    """Compute SHA256 hash of file content."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return f"sha256:{sha256.hexdigest()}"


def run(ctx: dict, params: dict) -> str:
    """
    Ingest a file from wild to raw using DuckDB CSV reader.
    """
    utid = ctx["utid"]
    manifest = ctx.get("manifest", {})
    manifest_id = ctx.get("manifest_id", "unknown")
    manifest_version = ctx.get("manifest_version", "1.0.0")
    
    parser = get_parser("curation", manifest)
    evolution = parser.get_evolution()
    agency = parser.extract_agency()
    
    manifest_schema_version = evolution.manifest_schema_version
    data_schema_version = evolution.data_schema_version
    
    source = params.get("source_url")
    target_dir = params.get("target_path")
    
    source = _resolve_storage_path(source)
    target_dir = _resolve_storage_path(target_dir)
    
    original_filename = os.path.basename(source)
    base_name, _ = os.path.splitext(original_filename)
    
    content_hash = _compute_hash(source)
    
    # Use DuckDB CSV reader or fallback
    engine_tag = "🦆 DuckDB"
    if _DUCKDB_AVAILABLE:
        try:
            data = _parse_csv_duckdb(source)
        except Exception as e:
            print(f"      ⚠️  DuckDB CSV failed: {e}, using fallback")
            data = _parse_csv_fallback(source)
            engine_tag = "⚠️ DuckDB (fallback)"
    else:
        data = _parse_csv_fallback(source)
        engine_tag = "⚠️ DuckDB (fallback)"
    
    doc_id = f"doc-{uuid.uuid4()}"
    
    output = {
        "metadata": {
            "utid": utid,
            "doc_id": doc_id,
            "manifest_id": manifest_id,
            "manifest_version": manifest_version,
            "manifest_schema_version": manifest_schema_version,
            "data_schema_version": data_schema_version,
            "content_hash": content_hash,
            "source": original_filename,
            "record_count": len(data),
            "engine": "python_duckdb",
            "created_at": datetime.now(timezone.utc).isoformat()
        },
        "data": data
    }
    
    raw_filename = format_filename("raw", utid, f"_{agency}_{base_name}_v{manifest_version}") + ".json"
    destination = os.path.join(target_dir, raw_filename)
    
    os.makedirs(target_dir, exist_ok=True)
    
    with open(destination, "w") as f:
        json.dump(output, f, indent=2)
    
    ctx["ingested_file"] = destination
    ctx["doc_id"] = doc_id
    
    return f"INGEST_SUCCESS: {original_filename} -> {os.path.basename(destination)} ({engine_tag})"


run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.ingest.v1"
}
