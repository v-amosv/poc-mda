# platform/execution_plane/engines/curation_engine/python_spark/v1/ingest_default.py
"""
V1 Default Ingestion Component (PySpark Engine)

Moves data from 'wild' (external source) to 'raw' (landing zone).
Uses PySpark's native CSV reader for parsing, demonstrating engine-native tools.

Spark Operations Used:
- spark.read.csv() - Native CSV parsing with schema inference
- DataFrame.collect() - Convert to Python for JSON envelope wrapping
"""
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Import PySpark (no longer shadowed by our mda_platform directory)
try:
    from pyspark.sql import SparkSession
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# Resolve project root for relative paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"

sys.path.insert(0, str(_PROJECT_ROOT))
from mda_platform.execution_plane.common.connectors.sequence_counter import format_filename
from mda_platform.control_plane.registry.parser_registry import get_parser

# Lazy SparkSession
_spark_session = None


def _get_spark() -> "SparkSession":
    """Get or create SparkSession (singleton)."""
    global _spark_session
    if _spark_session is None and _PYSPARK_AVAILABLE:
        _spark_session = (
            SparkSession.builder
            .appName("MDA-POC-Ingest")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _spark_session.sparkContext.setLogLevel("ERROR")
    return _spark_session


def _resolve_storage_path(path: str) -> str:
    """Resolve storage paths to the new structure."""
    if path.startswith("wild/"):
        return str(_STORAGE_PLANE / path)
    if path.startswith("raw/"):
        return str(_STORAGE_PLANE / path)
    if not os.path.isabs(path):
        return str(_PROJECT_ROOT / path)
    return path


def _parse_csv_spark(file_path: str) -> list:
    """
    Parse CSV using PySpark's native CSV reader.
    Uses spark.read.csv() with inferSchema for type detection.
    """
    spark = _get_spark()
    if spark is None:
        raise ImportError("PySpark not available")
    
    # Use Spark's native CSV reader with schema inference
    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True,  # Auto-detect types (int, float, string)
        mode="PERMISSIVE"
    )
    
    # Convert DataFrame to list of dicts
    return [row.asDict() for row in df.collect()]


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
    Ingest a file from wild to raw using PySpark CSV reader.
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
    
    # Use PySpark CSV reader or fallback
    engine_tag = "🔥 PySpark"
    if _PYSPARK_AVAILABLE:
        try:
            data = _parse_csv_spark(source)
        except Exception as e:
            print(f"      ⚠️  Spark CSV failed: {e}, using fallback")
            data = _parse_csv_fallback(source)
            engine_tag = "⚠️ PySpark (fallback)"
    else:
        data = _parse_csv_fallback(source)
        engine_tag = "⚠️ PySpark (fallback)"
    
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
            "engine": "python_spark",
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
