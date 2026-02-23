#!/usr/bin/env python3
# mda_platform/execution_plane/engines/semantic_engine/v1/ontology_mapper.py
"""
V1 Ontology Mapper Component

Generic JSON-to-JSON Transformer that:
1. Renames physical fields to semantic concepts
2. Adds context metadata (units, frequency, etc.)

The component has NO hardcoded field names - all logic is declarative
and driven by the manifest's projection.mapping configuration.

Example mapping:
  - { source_key: "series_id", target_concept: "indicator_code" }
  - { source_key: "value",     target_concept: "measurement_value" }
"""
from typing import Dict, List, Any


def run(ctx: dict, params: dict) -> str:
    """
    Execute ontology mapping: Project physical fields to semantic concepts.
    
    Args:
        ctx: Execution context containing:
            - source_fact: Dict with fact data from curation layer
        params: Component parameters:
            - mapping: List of {source_key, target_concept} dicts
            - context: Semantic context (unit_system, frequency, etc.)
    
    Returns:
        Success message
        
    Side effects:
        - Sets ctx["projected_data"] with mapped records
    """
    source_fact = ctx.get("source_fact", {})
    source_data = source_fact.get("data", [])
    
    if not source_data:
        ctx["projected_data"] = []
        return "MAP_SKIPPED: No source data"
    
    mapping = params.get("mapping", [])
    context = params.get("context", {})
    
    # Build mapping dictionary: source_key -> target_concept
    field_map = {}
    for m in mapping:
        source_key = m.get("source_key")
        target_concept = m.get("target_concept")
        if source_key and target_concept:
            field_map[source_key] = target_concept
    
    # Project each record
    projected_records = []
    mapped_count = 0
    
    for record in source_data:
        projected = {}
        
        # Apply field mapping
        for source_key, value in record.items():
            if source_key in field_map:
                # Map to semantic concept
                projected[field_map[source_key]] = value
                mapped_count += 1
            else:
                # Pass through unmapped fields with prefix
                projected[f"_raw_{source_key}"] = value
        
        # Add semantic context as metadata
        projected["__context__"] = context
        
        projected_records.append(projected)
    
    ctx["projected_data"] = projected_records
    
    return f"MAP_SUCCESS: {len(projected_records)} records, {len(field_map)} fields mapped"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.semantics.mapper.v1"
}
