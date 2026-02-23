# platform/execution_plane/engines/curation_engine/python_spark/v1/csv_parser.py
"""
V1 PySpark CSV Parser Component

Reads parsed data from the 'raw' zone JSON envelope.
SPARK ENGINE BEHAVIOR: Uses PySpark DataFrame with upper() transformation.

This engine uses a real SparkSession to process data, demonstrating
that the MDA can swap between different execution engines seamlessly.
"""
import json
import os
import sys
from pathlib import Path

# Import PySpark (no longer shadowed by our mda_platform directory)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, upper
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

# Resolve project root for relative paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent

# Lazy SparkSession initialization
_spark_session = None


def _get_spark():
    """Get or create a SparkSession (singleton pattern)."""
    global _spark_session
    if _spark_session is None and _PYSPARK_AVAILABLE:
        _spark_session = (
            SparkSession.builder
            .appName("MDA-POC-CurationEngine")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.enabled", "false")  # Disable Spark UI for POC
            .getOrCreate()
        )
        # Reduce logging verbosity
        _spark_session.sparkContext.setLogLevel("WARN")
    return _spark_session


def _uppercase_transform_fallback(data: list) -> list:
    """Fallback UPPERCASE transform when PySpark not available."""
    transformed = []
    for row in data:
        new_row = {}
        for key, value in row.items():
            if isinstance(value, str):
                new_row[key] = value.upper()
            else:
                new_row[key] = value
        transformed.append(new_row)
    return transformed


def _uppercase_transform_spark(data: list) -> list:
    """
    Transform all string values to UPPERCASE using PySpark DataFrame.
    This demonstrates real Spark processing within the MDA engine.
    Falls back to pure Python if PySpark/Java not available.
    """
    if not data:
        return data
    
    if not _PYSPARK_AVAILABLE:
        print("      ⚠️  PySpark not available, using fallback transform")
        return _uppercase_transform_fallback(data)
    
    try:
        spark = _get_spark()
        
        # Create DataFrame from list of dicts
        df = spark.createDataFrame(data)
        
        # Apply upper() to all string columns
        for field in df.schema.fields:
            if str(field.dataType) == "StringType()":
                df = df.withColumn(field.name, upper(col(field.name)))
        
        # Convert back to list of dicts
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        # PySpark imported but Java runtime not available
        print(f"      ⚠️  PySpark/Java runtime error: {str(e)[:50]}, using fallback transform")
        return _uppercase_transform_fallback(data)


def run(ctx: dict, params: dict) -> str:
    """
    Read parsed data from raw JSON envelope.
    Applies UPPERCASE transformation using PySpark DataFrame.
    
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
    
    # SPARK ENGINE: Apply UPPERCASE transformation via PySpark
    parsed_data = _uppercase_transform_spark(parsed_data)
    
    # Store parsed data in context for downstream components
    ctx["parsed_data"] = parsed_data
    
    row_count = len(parsed_data)
    col_count = len(parsed_data[0].keys()) if parsed_data else 0
    filename = os.path.basename(ingested_file)
    
    mode_tag = " [REPLAY]" if replay_mode else ""
    return f"PARSE_SUCCESS{mode_tag}: {row_count} rows x {col_count} cols from {filename} (🔥 PySpark - UPPERCASE)"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.parse.v1"
}
