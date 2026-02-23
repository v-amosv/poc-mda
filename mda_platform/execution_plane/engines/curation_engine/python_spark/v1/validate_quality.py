# platform/execution_plane/engines/curation_engine/python_spark/v1/validate_quality.py
"""
V1 Data Quality Validation Component (PySpark Engine)

Validates data against data_model quality checks using PySpark DataFrame operations.

Spark Operations Used:
- spark.createDataFrame() - Convert data to DataFrame
- df.filter() - Apply quality check conditions
- df.count() - Count failures using Spark aggregation
- col(), isNull(), isNotNull() - Column operations
"""
import sys
from pathlib import Path
from typing import Any

# Import PySpark (no longer shadowed by our mda_platform directory)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, count
    _PYSPARK_AVAILABLE = True
except ImportError:
    _PYSPARK_AVAILABLE = False

_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from mda_platform.control_plane.registry.parser_registry import get_parser

# Lazy SparkSession
_spark_session = None


def _get_spark() -> "SparkSession":
    """Get or create SparkSession (singleton)."""
    global _spark_session
    if _spark_session is None and _PYSPARK_AVAILABLE:
        _spark_session = (
            SparkSession.builder
            .appName("MDA-POC-Validate")
            .master("local[*]")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        _spark_session.sparkContext.setLogLevel("ERROR")
    return _spark_session


def _validate_with_spark(data: list, data_model: dict) -> tuple:
    """
    Validate data using PySpark DataFrame operations.
    Returns (validated_columns, total_checks, total_passed, total_failed)
    """
    spark = _get_spark()
    if spark is None or not data:
        raise ImportError("PySpark not available")
    
    df = spark.createDataFrame(data)
    total_rows = df.count()
    
    validated_columns = []
    total_checks = 0
    total_passed = 0
    total_failed = 0
    
    for col_def in data_model.get("columns", []):
        col_name = col_def["column_name"]
        quality_checks = col_def.get("quality_checks", [])
        
        # Skip if column not in data
        if col_name not in df.columns:
            continue
        
        col_passed = True
        col_failed_count = 0
        check_results = []
        
        for check in quality_checks:
            check_type = check.get("check")
            check_params = check.get("params", {})
            
            # Get the column's data type to handle type-specific checks
            col_type = str(df.schema[col_name].dataType)
            is_string_type = "StringType" in col_type
            
            # Use Spark filter operations for each check type
            if check_type == "not_null":
                # Count nulls using Spark's isNull()
                # Only check for empty string on string columns
                if is_string_type:
                    null_count = df.filter(col(col_name).isNull() | (col(col_name) == "")).count()
                else:
                    null_count = df.filter(col(col_name).isNull()).count()
                failed_records = null_count
                
            elif check_type == "positive":
                # Count non-positive values using Spark filter
                failed_records = df.filter(
                    col(col_name).isNull() | (col(col_name) <= 0)
                ).count()
                
            elif check_type == "range":
                min_val = check_params.get("min", float("-inf"))
                max_val = check_params.get("max", float("inf"))
                # Count out-of-range values using Spark filter
                failed_records = df.filter(
                    col(col_name).isNull() | 
                    (col(col_name) < min_val) | 
                    (col(col_name) > max_val)
                ).count()
            else:
                failed_records = 0
            
            check_passed = failed_records == 0
            total_checks += 1
            
            if check_passed:
                total_passed += 1
            else:
                total_failed += 1
                col_passed = False
                col_failed_count += failed_records
            
            check_results.append({
                "check": check_type,
                "passed": check_passed,
                "failed_count": failed_records
            })
        
        validated_col = {
            "column_name": col_name,
            "data_type": col_def.get("data_type"),
            "constraints": col_def.get("constraints", {}),
            "semantic_definition": col_def.get("semantic_definition", ""),
            "data_quality": {
                "passed": col_passed,
                "failed_count": col_failed_count,
                "checks": check_results
            }
        }
        validated_columns.append(validated_col)
    
    return validated_columns, total_checks, total_passed, total_failed


def _validate_fallback(data: list, data_model: dict) -> tuple:
    """Fallback validation using standard Python."""
    
    def _check_not_null(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str) and value.strip() == "":
            return False
        return True
    
    def _check_positive(value: Any) -> bool:
        if value is None:
            return False
        try:
            return float(value) > 0
        except (ValueError, TypeError):
            return False
    
    def _check_range(value: Any, params: dict) -> bool:
        if value is None:
            return False
        try:
            val = float(value)
            return params.get("min", float("-inf")) <= val <= params.get("max", float("inf"))
        except (ValueError, TypeError):
            return False
    
    validated_columns = []
    total_checks = 0
    total_passed = 0
    total_failed = 0
    
    for col_def in data_model.get("columns", []):
        col_name = col_def["column_name"]
        quality_checks = col_def.get("quality_checks", [])
        
        col_passed = True
        col_failed_count = 0
        check_results = []
        
        for check in quality_checks:
            check_type = check.get("check")
            check_params = check.get("params", {})
            
            failed_records = 0
            for record in data:
                value = record.get(col_name)
                if check_type == "not_null" and not _check_not_null(value):
                    failed_records += 1
                elif check_type == "positive" and not _check_positive(value):
                    failed_records += 1
                elif check_type == "range" and not _check_range(value, check_params):
                    failed_records += 1
            
            check_passed = failed_records == 0
            total_checks += 1
            
            if check_passed:
                total_passed += 1
            else:
                total_failed += 1
                col_passed = False
                col_failed_count += failed_records
            
            check_results.append({
                "check": check_type,
                "passed": check_passed,
                "failed_count": failed_records
            })
        
        validated_col = {
            "column_name": col_name,
            "data_type": col_def.get("data_type"),
            "constraints": col_def.get("constraints", {}),
            "semantic_definition": col_def.get("semantic_definition", ""),
            "data_quality": {
                "passed": col_passed,
                "failed_count": col_failed_count,
                "checks": check_results
            }
        }
        validated_columns.append(validated_col)
    
    return validated_columns, total_checks, total_passed, total_failed


def run(ctx: dict, params: dict) -> str:
    """
    Validate data using PySpark DataFrame operations.
    """
    manifest = ctx.get("manifest", {})
    parser = get_parser("curation", manifest)
    data_model = parser.load_data_model()
    
    if not data_model:
        ctx["data_model_validated"] = None
        return "VALIDATE_SKIPPED: No data_model defined"
    
    data = ctx.get("enriched_data") or ctx.get("parsed_data", [])
    
    if not data:
        ctx["data_model_validated"] = data_model
        return "VALIDATE_SKIPPED: No data to validate"
    
    # Use PySpark validation or fallback
    engine_tag = "🔥 PySpark"
    if _PYSPARK_AVAILABLE:
        try:
            validated_columns, total_checks, total_passed, total_failed = _validate_with_spark(data, data_model)
        except Exception as e:
            print(f"      ⚠️  Spark validation failed: {e}, using fallback")
            validated_columns, total_checks, total_passed, total_failed = _validate_fallback(data, data_model)
            engine_tag = "⚠️ PySpark (fallback)"
    else:
        validated_columns, total_checks, total_passed, total_failed = _validate_fallback(data, data_model)
        engine_tag = "⚠️ PySpark (fallback)"
    
    validated_data_model = {
        "schema_version": data_model.get("schema_version", "1.0.0"),
        "semantic_definition": data_model.get("semantic_definition", ""),
        "column_count": data_model.get("column_count", len(validated_columns)),
        "columns": validated_columns
    }
    
    ctx["data_model_validated"] = validated_data_model
    
    overall_passed = total_failed == 0
    status = "PASSED" if overall_passed else "FAILED"
    
    return f"VALIDATE_SUCCESS: {status} - {total_passed}/{total_checks} checks ({engine_tag})"


run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.validate.v1"
}


run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.validate.v1"
}
