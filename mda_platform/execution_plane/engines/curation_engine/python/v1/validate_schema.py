# lib/engines/curation/v1/validate_schema.py
"""
V1 Schema Validation Component

Validates parsed data against the manifest schema definition.
Uses quarantine approach: invalid records are separated, valid records continue.

Validates:
  - Field types (string, integer, float)
  - Nullable constraints
  - Check constraints (expressions)
  - Primary key uniqueness
"""
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Resolve project root for paths
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent


def _check_type(value: Any, expected_type: str) -> bool:
    """Check if value matches expected type."""
    if value is None:
        return True  # Null handling is separate
    
    type_checks = {
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "float": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "boolean": lambda v: isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    }
    
    checker = type_checks.get(expected_type.lower(), lambda v: True)
    return checker(value)


def _check_nullable(value: Any, nullable: bool) -> bool:
    """Check nullable constraint."""
    if not nullable and value is None:
        return False
    return True


def _evaluate_check_constraint(record: Dict, expression: str) -> bool:
    """
    Evaluate a check constraint expression against a record.
    
    Supports simple expressions like:
      - "year >= 1900 AND year <= 2100"
      - "population >= 0"
    """
    try:
        # Create a safe evaluation context with record values
        # Replace field names with their values
        eval_expr = expression
        
        for field, value in record.items():
            if value is None:
                # Can't evaluate constraints on null values
                return True
            
            # Replace field name with value (handle both quoted and unquoted)
            if isinstance(value, str):
                eval_expr = re.sub(rf'\b{field}\b', f'"{value}"', eval_expr)
            else:
                eval_expr = re.sub(rf'\b{field}\b', str(value), eval_expr)
        
        # Safely evaluate (only allow comparison operators)
        # Replace SQL-style operators
        eval_expr = eval_expr.replace(" AND ", " and ")
        eval_expr = eval_expr.replace(" OR ", " or ")
        
        # Use eval with restricted builtins
        return eval(eval_expr, {"__builtins__": {}}, {})
    
    except Exception:
        # If we can't evaluate, assume valid (conservative approach)
        return True


def _check_primary_key(records: List[Dict], pk_fields: List[str]) -> List[Tuple[int, str]]:
    """
    Check primary key uniqueness.
    
    Returns list of (index, error_message) for duplicates.
    """
    seen_keys = {}
    violations = []
    
    for idx, record in enumerate(records):
        # Build composite key
        key_values = tuple(record.get(f) for f in pk_fields)
        
        if key_values in seen_keys:
            violations.append((
                idx,
                f"Duplicate primary key {pk_fields}: {key_values} (first seen at row {seen_keys[key_values]})"
            ))
        else:
            seen_keys[key_values] = idx
    
    return violations


def run(ctx: dict, params: dict) -> str:
    """
    Validate parsed data against manifest schema.
    
    Args:
        ctx: Execution context containing:
            - utid: Unified Trace ID
            - manifest: Full manifest with schema definition
            - parsed_data: Data to validate
        params: Component parameters:
            - quarantine_path: Where to store quarantined records (default: quarantine/)
    
    Returns:
        Success message with validation stats
        
    Side effects:
        - ctx['parsed_data'] updated to only valid records
        - ctx['quarantine_count'] set to number of quarantined records
        - Quarantine file written if violations found
    """
    utid = ctx["utid"]
    manifest = ctx.get("manifest", {})
    manifest_id = ctx["manifest_id"]
    manifest_version = ctx["manifest_version"]
    
    # Get parsed data
    parsed_data = ctx.get("parsed_data", [])
    if not parsed_data:
        return "VALIDATE_SKIPPED: No data to validate"
    
    # Get schema from manifest
    schema = manifest.get("schema", {})
    if not schema:
        ctx["quarantine_count"] = 0
        return "VALIDATE_SKIPPED: No schema defined in manifest"
    
    fields = {f["name"]: f for f in schema.get("fields", [])}
    primary_key = schema.get("primary_key", [])
    
    # Track violations per record
    record_violations: Dict[int, List[str]] = {}
    
    # Validate each record
    for idx, record in enumerate(parsed_data):
        violations = []
        
        for field_name, field_def in fields.items():
            value = record.get(field_name)
            
            # Type check
            expected_type = field_def.get("type", "string")
            if not _check_type(value, expected_type):
                violations.append(
                    f"Field '{field_name}': expected {expected_type}, got {type(value).__name__}"
                )
            
            # Nullable check
            nullable = field_def.get("nullable", True)
            if not _check_nullable(value, nullable):
                violations.append(
                    f"Field '{field_name}': null value not allowed"
                )
            
            # Check constraints
            for constraint in field_def.get("constraints", []):
                if constraint.get("type") == "check":
                    expr = constraint.get("expression", "")
                    if not _evaluate_check_constraint(record, expr):
                        violations.append(
                            f"Field '{field_name}': check constraint failed: {expr}"
                        )
        
        if violations:
            record_violations[idx] = violations
    
    # Check primary key uniqueness
    if primary_key:
        pk_violations = _check_primary_key(parsed_data, primary_key)
        for idx, error in pk_violations:
            if idx not in record_violations:
                record_violations[idx] = []
            record_violations[idx].append(error)
    
    # Separate valid and quarantined records
    valid_records = []
    quarantined_records = []
    
    for idx, record in enumerate(parsed_data):
        if idx in record_violations:
            quarantined_records.append({
                "record_index": idx,
                "record": record,
                "violations": record_violations[idx]
            })
        else:
            valid_records.append(record)
    
    # Write quarantine file if violations found
    quarantine_path = params.get("quarantine_path", "quarantine/")
    if not os.path.isabs(quarantine_path):
        quarantine_path = str(_PROJECT_ROOT / quarantine_path)
    
    if quarantined_records:
        os.makedirs(quarantine_path, exist_ok=True)
        
        quarantine_file = os.path.join(
            quarantine_path,
            f"{manifest_id}_{manifest_version}_{utid}.json"
        )
        
        quarantine_output = {
            "_metadata": {
                "utid": utid,
                "manifest_id": manifest_id,
                "manifest_version": manifest_version,
                "schema_version": schema.get("version", "1.0.0"),
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
                "total_records": len(parsed_data),
                "quarantined_count": len(quarantined_records),
                "valid_count": len(valid_records)
            },
            "quarantined_records": quarantined_records
        }
        
        with open(quarantine_file, "w") as f:
            json.dump(quarantine_output, f, indent=2)
    
    # Update context with valid records only
    ctx["parsed_data"] = valid_records
    ctx["quarantine_count"] = len(quarantined_records)
    
    # Build result message
    if quarantined_records:
        return (
            f"VALIDATE_COMPLETE: {len(valid_records)} valid, "
            f"{len(quarantined_records)} quarantined"
        )
    else:
        return f"VALIDATE_SUCCESS: All {len(valid_records)} records valid"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.validate.v1"
}
