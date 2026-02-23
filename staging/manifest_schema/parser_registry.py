# staging/manifest_schema/parser_registry.py
"""
Manifest Parser Registry

Routes manifest parsing to the correct layer-specific versioned parser.
This registry is copied to control_plane at first onboarding.

Usage:
    from mda_platform.control_plane.registry.parser_registry import get_parser
    
    parser = get_parser(layer, manifest)
    agency = parser.extract_agency()
    data_model = parser.load_data_model()
"""
import importlib
import sys
from pathlib import Path
from typing import Any


class ManifestParserRegistry:
    """
    Registry of layer-specific versioned manifest parsers.
    
    Routes to correct parser based on layer and manifest_schema_version.
    Uses dynamic imports to support parsers being added at runtime.
    """
    
    @classmethod
    def get_parser(cls, layer: str, manifest: dict) -> Any:
        """
        Get the appropriate parser for a manifest.
        
        Args:
            layer: Layer name (curation, semantics, retrieval)
            manifest: The manifest dict
            
        Returns:
            Versioned parser instance
            
        Raises:
            ValueError: If no parser supports the schema version
            ImportError: If parser module not found
        """
        # Extract schema version from manifest
        if "manifest" in manifest:
            schema_version = manifest["manifest"].get("evolution", {}).get("manifest_schema_version", "2.0.0")
        else:
            schema_version = manifest.get("evolution", {}).get("manifest_schema_version", "1.0.0")
        
        major = int(schema_version.split(".")[0])
        
        # Build import path: registry/{layer}/schema/parsers/v{major}
        module_path = f"mda_platform.control_plane.registry.{layer}.schema.parsers.v{major}.manifest_parser"
        
        try:
            # Invalidate cache to pick up newly onboarded parsers
            if module_path in sys.modules:
                del sys.modules[module_path]
            
            module = importlib.import_module(module_path)
            parser_class = getattr(module, f"ManifestParserV{major}")
            return parser_class(manifest)
        except ImportError as e:
            raise ImportError(
                f"Parser not found for {layer} v{major}. "
                f"Expected module: {module_path}. "
                f"Onboard a manifest first to install the parser. Error: {e}"
            )
        except AttributeError as e:
            raise ValueError(
                f"Parser class ManifestParserV{major} not found in {module_path}. Error: {e}"
            )
    
    @classmethod
    def supported_versions(cls, layer: str) -> list:
        """Get list of supported major versions for a layer."""
        # Check which parser versions exist
        registry_path = Path(__file__).parent / layer / "schema" / "parsers"
        if not registry_path.exists():
            return []
        
        versions = []
        for v_dir in registry_path.iterdir():
            if v_dir.is_dir() and v_dir.name.startswith("v"):
                try:
                    versions.append(int(v_dir.name[1:]))
                except ValueError:
                    pass
        return sorted(versions)


def get_parser(layer: str, manifest: dict) -> Any:
    """
    Convenience function to get parser for a manifest.
    
    Args:
        layer: Layer name (curation, semantics, retrieval)
        manifest: The manifest dict
        
    Returns:
        Versioned parser instance
    """
    return ManifestParserRegistry.get_parser(layer, manifest)
