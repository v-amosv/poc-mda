# platform/execution_plane/engines/curation_engine/python_spark/v1/fact_store_writer.py
"""
V1 Fact Store Writer Component (PySpark Engine)

Writes enriched data to the Fact Store as JSON.
Note: For POC, this simulates Spark-like behavior using standard Python.
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"

sys.path.insert(0, str(_PROJECT_ROOT))

from mda_platform.control_plane.registry.parser_registry import get_parser
from mda_platform.execution_plane.common.connectors.sequence_counter import format_filename


def _resolve_storage_path(path: str, agency: str = None) -> str:
    base_path = _STORAGE_PLANE / "fact_store"
    if agency:
        return str(base_path / agency)
    return str(base_path)


def run(ctx: dict, params: dict) -> str:
    """
    Write data to the Fact Store with metadata header.
    """
    utid = ctx["utid"]
    manifest_id = ctx["manifest_id"]
    manifest_version = ctx["manifest_version"]
    manifest = ctx.get("manifest", {})
    
    data = ctx.get("enriched_data") or ctx.get("parsed_data", [])
    
    if not data:
        return "WRITE_SKIPPED: No data to write"
    
    output_format = params.get("format", "json")
    
    parser = get_parser("curation", manifest)
    agency = parser.extract_agency()
    
    output_path = _resolve_storage_path("fact-store/", agency)
    os.makedirs(output_path, exist_ok=True)
    
    suffix = f"_{manifest_id}_v{manifest_version}"
    filename = format_filename("fact", utid, suffix) + f".{output_format}"
    full_path = os.path.join(output_path, filename)
    
    evolution = parser.get_evolution()
    manifest_schema_version = evolution.manifest_schema_version
    data_schema_version = evolution.data_schema_version
    engine = evolution.engine
    engine_version = evolution.engine_version
    
    doc_id = ctx.get("doc_id", "unknown")
    
    ingested_file = ctx.get("ingested_file", "")
    source = os.path.basename(ingested_file) if ingested_file else "unknown"
    
    data_json = json.dumps(data, sort_keys=True)
    content_hash = f"sha256:{hashlib.sha256(data_json.encode()).hexdigest()}"
    
    data_model_validated = ctx.get("data_model_validated")
    
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
    
    if data_model_validated:
        output["metadata"]["data_model"] = data_model_validated
    
    with open(full_path, "w") as f:
        json.dump(output, f, indent=2)
    
    ctx["fact_store_path"] = full_path
    
    return f"WRITE_SUCCESS: {len(data)} records -> {filename} (SPARK engine)"


run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.output.v1"
}
