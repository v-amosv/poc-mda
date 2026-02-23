# platform/execution_plane/lib/engines/curation/v1/ingest_default.py
"""
V1 Default Ingestion Component

Moves data from 'wild' (external source) to 'raw' (landing zone).
Parses source file and wraps in JSON envelope with metadata for traceability.

Filename format: raw-{seq:04d}-utid-{utid}_{agency}_{original_filename}_v{version}.json
Example: raw-0001-utid-5fe5ccab-838d-4d1c-81b4-50b1c236d3c5_bls_employment_stats_v1.0.0.json

Raw file structure:
{
  "metadata": {
    "utid": "...",
    "doc_id": "doc-...",
    "manifest_id": "...",
    "manifest_version": "...",
    "manifest_schema_version": "...",
    "data_schema_version": "...",
    "content_hash": "sha256:...",
    "source_file": "...",
    "record_count": N,
    "created_at": "..."
  },
  "data": [...]
}
"""
import csv
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Resolve project root for relative paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"

# Import sequence counter and Control Plane parser
import sys
sys.path.insert(0, str(_PROJECT_ROOT))
from mda_platform.execution_plane.common.connectors.sequence_counter import format_filename
from mda_platform.control_plane.registry.parser_registry import get_parser


def _resolve_storage_path(path: str) -> str:
    """
    Resolve storage paths to the new structure.
    wild/... -> platform/storage_plane/wild/...
    raw/...  -> platform/storage_plane/raw/...
    """
    if path.startswith("wild/"):
        return str(_STORAGE_PLANE / path)
    if path.startswith("raw/"):
        return str(_STORAGE_PLANE / path)
    if not os.path.isabs(path):
        return str(_PROJECT_ROOT / path)
    return path


def _parse_csv(file_path: str, delimiter: str = ",") -> list:
    """Parse CSV file into list of dicts."""
    with open(file_path, mode="r") as f:
        reader = csv.reader(f, delimiter=delimiter)
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
    Ingest a file from wild to raw with JSON envelope.
    
    Uses Control Plane's manifest parser for schema-agnostic manifest access.
    
    Args:
        ctx: Execution context containing:
            - utid: Unified Trace ID
            - manifest: Full manifest dict
        params: Component parameters:
            - source_url: Path to source file in wild/
            - target_path: Destination directory in raw/
    
    Returns:
        Success message with file paths
        
    Side effects:
        - Parses source file and wraps in JSON with metadata
        - Updates ctx['ingested_file'] for downstream components
    """
    utid = ctx["utid"]
    manifest = ctx.get("manifest", {})
    manifest_id = ctx.get("manifest_id", "unknown")
    manifest_version = ctx.get("manifest_version", "1.0.0")
    
    # Use Control Plane parser - Execution Plane is schema-agnostic
    parser = get_parser("curation", manifest)
    evolution = parser.get_evolution()
    agency = parser.extract_agency()
    
    manifest_schema_version = evolution.manifest_schema_version
    data_schema_version = evolution.data_schema_version
    
    source = params.get("source_url")
    target_dir = params.get("target_path")
    
    # Resolve to storage plane paths
    source = _resolve_storage_path(source)
    target_dir = _resolve_storage_path(target_dir)
    
    # Read and parse source file
    original_filename = os.path.basename(source)
    base_name, _ = os.path.splitext(original_filename)
    
    # Compute content hash before parsing
    content_hash = _compute_hash(source)
    
    # Parse CSV data
    data = _parse_csv(source)
    
    # Generate unique document ID
    doc_id = f"doc-{uuid.uuid4()}"
    
    # Build JSON envelope with metadata
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
            "created_at": datetime.now(timezone.utc).isoformat()
        },
        "data": data
    }
    
    # Create destination filename (now .json)
    # Format: raw-{seq:04d}-utid-{utid}_{agency}_{original_filename}_v{version}.json
    raw_filename = format_filename("raw", utid, f"_{agency}_{base_name}_v{manifest_version}") + ".json"
    destination = os.path.join(target_dir, raw_filename)
    
    # Ensure target directory exists
    os.makedirs(target_dir, exist_ok=True)
    
    # Write JSON file
    with open(destination, "w") as f:
        json.dump(output, f, indent=2)
    
    # Store ingested file path in context for downstream components
    ctx["ingested_file"] = destination
    ctx["doc_id"] = doc_id
    
    return f"INGEST_SUCCESS: {original_filename} -> {os.path.basename(destination)}"


# MDA Component Metadata
# Required for RuntimeResolver validation
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.ingest.v1"
}