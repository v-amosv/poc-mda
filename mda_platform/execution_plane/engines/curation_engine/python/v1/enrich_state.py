# lib/engines/curation/v1/enrich_state.py
"""
V1 State Enrichment Component

Adds state_code field to data based on state name mapping.
Uses reference data deployed to Manifest Store via Control Plane parser.
"""
import sys
from pathlib import Path
from typing import Dict, List, Any

# Resolve project root for imports
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

# Use Control Plane's manifest parser - Execution Plane is schema-agnostic
from mda_platform.control_plane.registry.parser_registry import get_parser


def run(ctx: dict, params: dict) -> str:
    """
    Enrich data with state code from reference mapping.
    
    Uses Control Plane's manifest parser for schema-agnostic manifest access.
    
    Args:
        ctx: Execution context containing:
            - utid: Unified Trace ID
            - parsed_data: List of dicts from csv_parser
            - manifest: Full manifest (for reference_data lookup)
        params: Component parameters:
            - source_field: Field containing state name (e.g., 'state')
            - target_field: Field to add with state code (e.g., 'state_code')
            - mapping_ref: Reference to mapping in manifest's reference_data
    
    Returns:
        Success message with enrichment stats
        
    Side effects:
        - Updates ctx['enriched_data'] with enriched records
    """
    source_field = params.get("source_field", "state")
    target_field = params.get("target_field", "state_code")
    mapping_ref = params.get("mapping_ref", "state_mappings")
    
    # Get parsed data from previous step
    parsed_data = ctx.get("parsed_data", [])
    if not parsed_data:
        return "ENRICH_SKIPPED: No parsed data to enrich"
    
    # Use Control Plane parser - Execution Plane is schema-agnostic
    manifest = ctx.get("manifest", {})
    parser = get_parser("curation", manifest)
    
    # Load reference data via parser (handles path resolution)
    ref_data = parser.load_reference_data(mapping_ref)
    
    if not ref_data:
        raise ValueError(
            f"Reference data '{mapping_ref}' not found. "
            f"Available: {parser.list_reference_data()}"
        )
    
    mappings = ref_data.get("mappings", {})
    
    # Create case-insensitive lookup (for v2 parser compatibility)
    mappings_lower = {k.lower(): v for k, v in mappings.items()}
    
    # Enrich each record
    enriched_count = 0
    unmapped_states = set()
    enriched_data = []
    
    for record in parsed_data:
        state_name = record.get(source_field)
        # Try exact match first, then case-insensitive
        state_code = mappings.get(state_name) or mappings_lower.get(str(state_name).lower() if state_name else "")
        
        enriched_record = record.copy()
        
        if state_code:
            enriched_record[target_field] = state_code
            enriched_count += 1
        else:
            enriched_record[target_field] = None
            if state_name:
                unmapped_states.add(state_name)
        
        enriched_data.append(enriched_record)
    
    # Store enriched data in context for next step
    ctx["enriched_data"] = enriched_data
    
    result = f"ENRICH_SUCCESS: Added {target_field} to {enriched_count}/{len(parsed_data)} records"
    
    if unmapped_states:
        result += f" (unmapped: {', '.join(sorted(unmapped_states))})"
    
    return result


# MDA Component Metadata
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.transform.v1"
}
