# platform/control-plane/registry/__init__.py
"""
Component Registry

Central catalog of available components with metadata:
- Version history
- Interface contracts
- Deprecation status
- Usage statistics

For POC: Simple in-memory registry.
Future: Backed by database or external service.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class ComponentInfo:
    """Metadata about a registered component."""
    path: str
    version: str
    interface: str
    description: str
    deprecated: bool = False


# In-memory registry for POC
_REGISTRY: Dict[str, List[ComponentInfo]] = {
    "lib.engines.curation.v1.csv_parser.parse_csv": [
        ComponentInfo(
            path="lib.engines.curation.v1.csv_parser.parse_csv",
            version="1.0.0",
            interface="mda.interfaces.parse.v1",
            description="CSV parser - preserves original case"
        )
    ],
    "lib.engines.curation.v2.csv_parser.parse_csv": [
        ComponentInfo(
            path="lib.engines.curation.v2.csv_parser.parse_csv",
            version="2.0.0",
            interface="mda.interfaces.parse.v1",
            description="CSV parser - normalizes strings to lowercase"
        )
    ],
    "lib.engines.curation.v1.field_mapper.run": [
        ComponentInfo(
            path="lib.engines.curation.v1.field_mapper.run",
            version="1.0.0",
            interface="mda.interfaces.transform.v1",
            description="Maps/renames fields based on configuration"
        )
    ],
    "lib.engines.curation.v1.enrich_state.run": [
        ComponentInfo(
            path="lib.engines.curation.v1.enrich_state.run",
            version="1.0.0",
            interface="mda.interfaces.enrich.v1",
            description="Enriches records with state codes from reference data"
        )
    ],
    "lib.engines.curation.v1.fact_store_writer.run": [
        ComponentInfo(
            path="lib.engines.curation.v1.fact_store_writer.run",
            version="1.0.0",
            interface="mda.interfaces.write.v1",
            description="Writes curated data to Fact Store"
        )
    ],
    "lib.engines.curation.v1.ingest_default.run": [
        ComponentInfo(
            path="lib.engines.curation.v1.ingest_default.run",
            version="1.0.0",
            interface="mda.interfaces.ingest.v1",
            description="Default ingestion from Wild to Raw"
        )
    ],
}


class Registry:
    """Component Registry interface."""
    
    @staticmethod
    def get(path: str, version: str = None) -> Optional[ComponentInfo]:
        """Get component info by path and optional version."""
        components = _REGISTRY.get(path, [])
        if not components:
            return None
        
        if version:
            for c in components:
                if c.version == version:
                    return c
            return None
        
        # Return latest (last in list)
        return components[-1]
    
    @staticmethod
    def list_all() -> List[ComponentInfo]:
        """List all registered components."""
        result = []
        for components in _REGISTRY.values():
            result.extend(components)
        return result
    
    @staticmethod
    def list_by_interface(interface: str) -> List[ComponentInfo]:
        """List components implementing a specific interface."""
        result = []
        for components in _REGISTRY.values():
            for c in components:
                if c.interface == interface:
                    result.append(c)
        return result
