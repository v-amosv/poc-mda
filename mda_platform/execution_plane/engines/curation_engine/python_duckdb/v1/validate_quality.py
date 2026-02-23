# platform/execution_plane/engines/curation_engine/python_duckdb/v1/validate_quality.py
"""
V1 Data Quality Validation Component (DuckDB Engine)

Validates data against data_model quality checks using DuckDB SQL queries.

DuckDB Operations Used:
- duckdb.connect() - In-memory database connection
- SQL COUNT(*) WHERE - Count failures using native SQL
- IS NULL, =, <, >, BETWEEN - SQL predicates for quality checks
"""
import sys
from pathlib import Path
from typing import Any

# DuckDB import
try:
    import duckdb
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from mda_platform.control_plane.registry.parser_registry import get_parser


def _validate_with_duckdb(data: list, data_model: dict) -> tuple:
    """
    Validate data using DuckDB SQL queries.
    Returns (validated_columns, total_checks, total_passed, total_failed)
    """
    if not data:
        raise ValueError("No data to validate")
    
    con = duckdb.connect(":memory:")
    
    # Register data as a table using pandas DataFrame
    import pandas as pd
    df = pd.DataFrame(data)
    con.register("input_data", df)
    
    # Get column names and types from the table
    table_columns = list(df.columns)
    
    # Get actual column types from DuckDB
    col_types = {}
    try:
        type_result = con.execute("DESCRIBE input_data").fetchall()
        for row in type_result:
            col_types[row[0]] = row[1]  # column_name -> type
    except Exception:
        pass  # If DESCRIBE fails, we'll treat all as potentially string
    
    validated_columns = []
    total_checks = 0
    total_passed = 0
    total_failed = 0
    
    for col_def in data_model.get("columns", []):
        col_name = col_def["column_name"]
        quality_checks = col_def.get("quality_checks", [])
        
        # Skip if column not in data (use quoted identifier for safety)
        if col_name not in table_columns:
            continue
        
        col_passed = True
        col_failed_count = 0
        check_results = []
        
        # Determine if column is numeric type (don't compare to empty string)
        col_type = col_types.get(col_name, "VARCHAR").upper()
        is_numeric = any(t in col_type for t in ["INT", "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "BIGINT", "SMALLINT", "TINYINT"])
        
        for check in quality_checks:
            check_type = check.get("check")
            check_params = check.get("params", {})
            
            # Build SQL query for each check type
            if check_type == "not_null":
                # For numeric types, only check IS NULL (not empty string comparison)
                if is_numeric:
                    sql = f'SELECT COUNT(*) FROM input_data WHERE "{col_name}" IS NULL'
                else:
                    sql = f'SELECT COUNT(*) FROM input_data WHERE "{col_name}" IS NULL OR "{col_name}" = \'\''
                
            elif check_type == "positive":
                # Use SQL to count non-positive values
                sql = f'SELECT COUNT(*) FROM input_data WHERE "{col_name}" IS NULL OR "{col_name}" <= 0'
                
            elif check_type == "range":
                min_val = check_params.get("min", float("-inf"))
                max_val = check_params.get("max", float("inf"))
                # Use SQL BETWEEN or comparison operators
                sql = f'SELECT COUNT(*) FROM input_data WHERE "{col_name}" IS NULL OR "{col_name}" < {min_val} OR "{col_name}" > {max_val}'
            else:
                sql = "SELECT 0"  # Unknown check type, no failures
            
            # Execute the SQL query
            failed_records = con.execute(sql).fetchone()[0]
            
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
    
    con.close()
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
    Validate data using DuckDB SQL queries.
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
    
    # Use DuckDB validation or fallback
    engine_tag = "🦆 DuckDB"
    if _DUCKDB_AVAILABLE:
        try:
            validated_columns, total_checks, total_passed, total_failed = _validate_with_duckdb(data, data_model)
        except Exception as e:
            print(f"      ⚠️  DuckDB validation failed: {e}, using fallback")
            validated_columns, total_checks, total_passed, total_failed = _validate_fallback(data, data_model)
            engine_tag = "⚠️ DuckDB (fallback)"
    else:
        validated_columns, total_checks, total_passed, total_failed = _validate_fallback(data, data_model)
        engine_tag = "⚠️ DuckDB (fallback)"
    
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
