# platform/execution_plane/lib/engines/curation/v1/validate_quality.py
"""
V1 Data Quality Validation Component

Validates data against data_model quality checks defined per column.
Adds validation results to data_model in context for downstream use.

Supported checks:
- not_null: Value must not be null/empty
- positive: Value must be > 0
- range: Value must be within min/max bounds
"""
import sys
from pathlib import Path
from typing import Any, Dict, List

# Resolve project root for imports
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

# Use Control Plane's manifest parser - Execution Plane is schema-agnostic
from mda_platform.control_plane.registry.parser_registry import get_parser


def _check_not_null(value: Any) -> bool:
    """Check if value is not null/empty."""
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == "":
        return False
    return True


def _check_positive(value: Any) -> bool:
    """Check if value is positive (> 0)."""
    if value is None:
        return False
    try:
        return float(value) > 0
    except (ValueError, TypeError):
        return False


def _check_range(value: Any, params: dict) -> bool:
    """Check if value is within range [min, max]."""
    if value is None:
        return False
    try:
        val = float(value)
        min_val = params.get("min", float("-inf"))
        max_val = params.get("max", float("inf"))
        return min_val <= val <= max_val
    except (ValueError, TypeError):
        return False


def _run_check(check_type: str, value: Any, params: dict = None) -> bool:
    """Run a single quality check."""
    if check_type == "not_null":
        return _check_not_null(value)
    elif check_type == "positive":
        return _check_positive(value)
    elif check_type == "range":
        return _check_range(value, params or {})
    else:
        # Unknown check type - pass by default
        return True


def run(ctx: dict, params: dict) -> str:
    """
    Validate data against data_model quality checks.
    
    Uses Control Plane's manifest parser for schema-agnostic manifest access.
    
    Args:
        ctx: Execution context containing:
            - manifest: Full manifest dict
            - enriched_data or parsed_data: Data to validate
        params: Component parameters (unused for now)
    
    Returns:
        Success message with validation summary
        
    Side effects:
        - Stores validated data_model with DQ results in ctx["data_model_validated"]
    """
    manifest = ctx.get("manifest", {})
    
    # Use Control Plane parser - Execution Plane is schema-agnostic
    parser = get_parser("curation", manifest)
    
    # Load data_model via parser (handles path resolution)
    data_model = parser.load_data_model()
    
    if not data_model:
        ctx["data_model_validated"] = None
        return "VALIDATE_SKIPPED: No data_model defined"
    
    # Get data to validate
    data = ctx.get("enriched_data") or ctx.get("parsed_data", [])
    
    if not data:
        ctx["data_model_validated"] = data_model
        return "VALIDATE_SKIPPED: No data to validate"
    
    # Build validated data_model with DQ results per column
    validated_columns = []
    total_checks = 0
    total_passed = 0
    total_failed = 0
    
    for col_def in data_model.get("columns", []):
        col_name = col_def["column_name"]
        quality_checks = col_def.get("quality_checks", [])
        
        # Track results for this column
        col_passed = True
        col_failed_count = 0
        check_results = []
        
        for check in quality_checks:
            check_type = check.get("check")
            check_params = check.get("params", {})
            
            # Run check on all records
            failed_records = 0
            for record in data:
                value = record.get(col_name)
                if not _run_check(check_type, value, check_params):
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
        
        # Build validated column entry
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
    
    # Build validated data_model
    validated_data_model = {
        "schema_version": data_model.get("schema_version", "1.0.0"),
        "semantic_definition": data_model.get("semantic_definition", ""),
        "column_count": data_model.get("column_count", len(validated_columns)),
        "columns": validated_columns
    }
    
    ctx["data_model_validated"] = validated_data_model
    
    overall_passed = total_failed == 0
    status = "PASSED" if overall_passed else "FAILED"
    
    return f"VALIDATE_SUCCESS: {status} - {total_passed}/{total_checks} checks passed"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.validate.v1"
}
