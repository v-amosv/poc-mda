# platform/execution_plane/lib/engines/curation/v1/fact_store_writer.py
"""
V1 Fact Store Writer Component

Writes enriched data to the Fact Store as JSON.
Output uses sequence-based naming for ordering and UTID for traceability.

Filename format: fact-{seq:04d}-utid-{utid}_{manifest_id}_v{manifest_version}.json
Example: fact-0001-utid-5fe5ccab-838d-4d1c-81b4-50b1c236d3c5_bls_employment_stats_v1.0.0.json

Structure: fact_store/{agency}/{filename}

Metadata schema (same as raw):
- utid, doc_id, manifest_id, manifest_version
- manifest_schema_version, data_schema_version
- content_hash, source, record_count, created_at
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any

# Resolve project root for imports
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"

sys.path.insert(0, str(_PROJECT_ROOT))

# Use Control Plane's manifest parser - Execution Plane is schema-agnostic
from mda_platform.control_plane.registry.parser_registry import get_parser
from mda_platform.execution_plane.common.connectors.sequence_counter import format_filename


def _resolve_storage_path(path: str, agency: str = None) -> str:
    """Resolve fact-store path to storage plane with agency structure."""
    base_path = _STORAGE_PLANE / "fact_store"
    if agency:
        return str(base_path / agency)
    return str(base_path)


def run(ctx: dict, params: dict) -> str:
    """
    Write data to the Fact Store with metadata header.
    
    Uses Control Plane's manifest parser for schema-agnostic manifest access.
    
    Args:
        ctx: Execution context containing:
            - utid: Unified Trace ID
            - manifest_id: Manifest identifier
            - manifest_version: Manifest version
            - enriched_data: Data to write (from enrich step)
            - parsed_data: Fallback if no enriched_data
        params: Component parameters:
            - output_path: Base path for fact store (e.g., 'fact-store/')
            - format: Output format ('json')
    
    Returns:
        Success message with output path
        
    Side effects:
        - Writes JSON file to fact-store/
    """
    utid = ctx["utid"]
    manifest_id = ctx["manifest_id"]
    manifest_version = ctx["manifest_version"]
    manifest = ctx.get("manifest", {})
    
    # Get data - prefer enriched, fall back to parsed
    data = ctx.get("enriched_data") or ctx.get("parsed_data", [])
    
    if not data:
        return "WRITE_SKIPPED: No data to write"
    
    output_format = params.get("format", "json")
    
    # Use Control Plane parser - Execution Plane is schema-agnostic
    parser = get_parser("curation", manifest)
    agency = parser.extract_agency()
    
    # Resolve to storage plane path with agency
    output_path = _resolve_storage_path("fact-store/", agency)
    
    # Ensure directory exists
    os.makedirs(output_path, exist_ok=True)
    
    # Sequence-based filename: fact-{seq:04d}-utid-{utid}_{manifest_id}_v{manifest_version}.json
    suffix = f"_{manifest_id}_v{manifest_version}"
    filename = format_filename("fact", utid, suffix) + f".{output_format}"
    full_path = os.path.join(output_path, filename)
    
    # Get version fields from manifest evolution block via parser
    evolution = parser.get_evolution()
    manifest_schema_version = evolution.manifest_schema_version
    data_schema_version = evolution.data_schema_version
    engine = evolution.engine
    engine_version = evolution.engine_version
    
    # Get doc_id from context (set by ingest step)
    doc_id = ctx.get("doc_id", "unknown")
    
    # Get source raw file path
    ingested_file = ctx.get("ingested_file", "")
    source = os.path.basename(ingested_file) if ingested_file else "unknown"
    
    # Compute content hash on fact data
    data_json = json.dumps(data, sort_keys=True)
    content_hash = f"sha256:{hashlib.sha256(data_json.encode()).hexdigest()}"
    
    # Get validated data_model with DQ results (set by validate_quality step)
    data_model_validated = ctx.get("data_model_validated")
    
    # Build output with metadata header (same schema as raw)
    output = {
        "metadata": {
            "utid": utid,
            "doc_id": doc_id,
            "manifest_id": manifest_id,
            "manifest_version": manifest_version,
            "manifest_schema_version": manifest_schema_version,
            "data_schema_version": data_schema_version,
            "engine": engine,
            "engine_version": engine_version,
            "content_hash": content_hash,
            "source": source,
            "record_count": len(data),
            "created_at": datetime.now(timezone.utc).isoformat()
        },
        "data": data
    }
    
    # Add data_model with DQ results if available
    if data_model_validated:
        output["metadata"]["data_model"] = data_model_validated
    
    # Write JSON
    with open(full_path, "w") as f:
        json.dump(output, f, indent=2)
    
    # Store output path in context
    ctx["fact_store_path"] = full_path
    
    return f"WRITE_SUCCESS: {len(data)} records -> {filename}"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.output.v1"
}
