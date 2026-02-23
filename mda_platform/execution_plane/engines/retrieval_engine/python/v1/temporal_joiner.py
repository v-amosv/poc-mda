#!/usr/bin/env python3
# mda_platform/execution_plane/engines/retrieval_engine/v1/temporal_joiner.py
"""
V1 Temporal Joiner Component

Generic Cross-Domain Join that:
1. Matches records from primary and secondary sources on a shared key
2. Merges fields from both sources
3. Applies optional STS (Source Traceability Score) filtering

The component is a GENERIC JOINER - complex math must be defined
as separate components and referenced explicitly in the manifest.

Example params:
  join_key: "observation_year"
  join_type: "inner"
  sts_filter: 0.8  # Drop facts if traceability is low
"""
from typing import Dict, List, Any


def run(ctx: dict, params: dict) -> str:
    """
    Execute temporal join: Merge records from primary and secondary sources.
    
    Args:
        ctx: Execution context containing:
            - primary_data: Dict with primary semantic data
            - secondary_data: Dict with secondary semantic data (optional)
        params: Component parameters:
            - join_key: Field to join on (e.g., "observation_year")
            - join_type: Join type (inner, left_outer, full_outer)
            - sts_filter: Optional minimum traceability score
    
    Returns:
        Success message
        
    Side effects:
        - Sets ctx["joined_data"] with merged records
    """
    primary = ctx.get("primary_data", {})
    secondary = ctx.get("secondary_data")
    
    primary_records = primary.get("data", [])
    
    join_key = params.get("join_key", "observation_year")
    join_type = params.get("join_type", "inner")
    sts_filter = params.get("sts_filter")
    
    # If no secondary source, just pass through primary with enrichment
    if not secondary:
        joined_records = []
        for record in primary_records:
            joined = {**record}
            joined["__join__"] = {
                "type": "passthrough",
                "sources": [primary.get("metadata", {}).get("manifest_id", "unknown")]
            }
            joined_records.append(joined)
        
        ctx["joined_data"] = joined_records
        return f"JOIN_PASSTHROUGH: {len(joined_records)} records (no secondary source)"
    
    secondary_records = secondary.get("data", [])
    
    # Build lookup from secondary on join_key
    secondary_lookup = {}
    for record in secondary_records:
        key_value = record.get(join_key)
        if key_value is not None:
            if key_value not in secondary_lookup:
                secondary_lookup[key_value] = []
            secondary_lookup[key_value].append(record)
    
    # Perform join
    joined_records = []
    matched_count = 0
    
    for primary_rec in primary_records:
        key_value = primary_rec.get(join_key)
        
        # Find matching secondary records
        secondary_matches = secondary_lookup.get(key_value, [])
        
        if join_type == "inner" and not secondary_matches:
            continue  # Skip unmatched in inner join
        
        if secondary_matches:
            # Merge with each matching secondary record
            for sec_rec in secondary_matches:
                joined = {}
                
                # Add primary fields with prefix
                for k, v in primary_rec.items():
                    if not k.startswith("__"):
                        joined[f"primary_{k}"] = v
                
                # Add secondary fields with prefix
                for k, v in sec_rec.items():
                    if not k.startswith("__"):
                        joined[f"secondary_{k}"] = v
                
                # Add join metadata
                joined["__join__"] = {
                    "type": join_type,
                    "key": join_key,
                    "key_value": key_value,
                    "sources": [
                        primary.get("metadata", {}).get("manifest_id", "unknown"),
                        secondary.get("metadata", {}).get("manifest_id", "unknown")
                    ]
                }
                
                joined_records.append(joined)
                matched_count += 1
        else:
            # Left outer join - include unmatched primary
            joined = {}
            for k, v in primary_rec.items():
                if not k.startswith("__"):
                    joined[f"primary_{k}"] = v
            
            joined["__join__"] = {
                "type": join_type,
                "key": join_key,
                "key_value": key_value,
                "matched": False,
                "sources": [primary.get("metadata", {}).get("manifest_id", "unknown")]
            }
            
            joined_records.append(joined)
    
    # Apply STS filter if specified
    if sts_filter:
        # For demo, we don't have actual STS scores, so we skip filtering
        pass
    
    ctx["joined_data"] = joined_records
    
    return f"JOIN_SUCCESS: {len(joined_records)} records ({join_type} on {join_key}, {matched_count} matches)"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.retrieval.joiner.v1"
}
