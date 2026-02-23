# lib/engines/curation/v1/field_mapper.py
"""
Field Mapper Component

Maps/renames fields from source schema to target schema.
Used when upstream data changes field names but downstream 
consumers expect the canonical schema.

MDA Component Metadata:
    __mda_component__ = True
    __mda_version__ = "1.0.0"
"""
from typing import List, Dict, Any

__mda_component__ = True
__mda_version__ = "1.0.0"


def run(ctx: dict, params: dict) -> str:
    """
    Rename fields according to mapping configuration.
    
    Args:
        ctx: Execution context containing parsed_data
        params: Component parameters:
            - mappings: Dict of {source_field: target_field}
            
    Returns:
        Success message
        
    Side effects:
        - Updates ctx['parsed_data'] with renamed fields
    """
    mappings = params.get("mappings", {})
    
    # Get parsed data from context
    parsed_data = ctx.get("parsed_data", [])
    
    if not mappings:
        print(f"   ⚠️  FIELD_MAPPER: No mappings defined, passing through")
        return "FIELD_MAP_SKIPPED: No mappings"
    
    if not parsed_data:
        return "FIELD_MAP_SKIPPED: No data"
    
    mapped_records = []
    
    for record in parsed_data:
        new_record = {}
        for key, value in record.items():
            # Apply mapping if exists, otherwise keep original
            new_key = mappings.get(key, key)
            new_record[new_key] = value
        mapped_records.append(new_record)
    
    # Update context with mapped data
    ctx["parsed_data"] = mapped_records
    
    mapped_fields = list(mappings.keys())
    print(f"   ✅ FIELD_MAP_SUCCESS: Mapped {len(mapped_fields)} field(s): {mappings}")
    
    return f"FIELD_MAP_SUCCESS: Mapped {mapped_fields}"


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.transform.v1"
}


if __name__ == "__main__":
    # Test
    test_data = [
        {"state": "California", "pop": 39538223},
        {"state": "Texas", "pop": 29145505}
    ]
    
    result = run(
        test_data,
        {"utid": "test"},
        {"mappings": {"pop": "population"}}
    )
    
    print(f"\nResult:")
    for row in result:
        print(f"  {row}")
