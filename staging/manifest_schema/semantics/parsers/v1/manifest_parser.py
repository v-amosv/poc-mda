# platform/control_plane/registry/{layer}/schema/parsers/v1/manifest_parser.py
"""
V1 Manifest Schema Parser (Semantics Layer)

This parser understands manifest_schema_version 1.x.x structure.
All manifest interpretation logic is centralized here.

The Execution Plane should NEVER directly access manifest fields.
Instead, it uses this parser to extract information.

V1 Schema Structure:
  - identity: {name, domain, owner_squad}
  - evolution: {manifest_version, manifest_schema_version, data_schema_version, engine_version}
  - reference_data: {<ref_name>: {path, version}, ...}
  - data_model: {path, version}
  - intent:
      - ingestion: {component: {path, version}, params: {...}}
      - processing: [{step, component: {path, version}, params: {...}}, ...]
"""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _find_project_root() -> Path:
    """Find project root by walking up to find pyproject.toml or mda_platform/."""
    current = Path(__file__).resolve().parent
    for _ in range(10):  # Max 10 levels up
        if (current / "pyproject.toml").exists() or (current / "mda_platform").is_dir():
            return current
        current = current.parent
    # Fallback to parent traversal (for staging context)
    return Path(__file__).parent.parent.parent.parent.parent


# Resolve project root for path resolution
_PROJECT_ROOT = _find_project_root()


@dataclass
class ComponentSpec:
    """Specification for a manifest component."""
    path: str
    version: str
    params: Dict[str, Any]


@dataclass
class ProcessingStep:
    """A processing step from manifest intent."""
    step_name: str
    component: ComponentSpec


@dataclass
class Evolution:
    """Manifest evolution block."""
    manifest_version: str
    manifest_schema_version: str
    data_schema_version: str
    engine: str  # Engine type: python, python_spark, python_duckdb, etc.
    engine_version: str


@dataclass
class Identity:
    """Manifest identity block."""
    name: str
    domain: str
    owner_squad: str


class ManifestParserV1:
    """
    V1 Manifest Parser - Single source of truth for V1 schema interpretation.
    
    Usage:
        parser = ManifestParserV1(manifest)
        agency = parser.extract_agency()
        data_model_path = parser.get_data_model_path()
        steps = parser.get_processing_steps()
    """
    
    # Schema version this parser handles
    SCHEMA_VERSION_MAJOR = 1
    
    def __init__(self, manifest: dict):
        """
        Initialize parser with a manifest dict.
        
        Args:
            manifest: The manifest dictionary to parse
            
        Raises:
            ValueError: If manifest is incompatible with V1 parser
        """
        self._manifest = manifest
        self._validate_schema_compatibility()
    
    def _validate_schema_compatibility(self) -> None:
        """Ensure this manifest is compatible with V1 parser."""
        schema_version = self._manifest.get("evolution", {}).get("manifest_schema_version", "1.0.0")
        major = int(schema_version.split(".")[0])
        
        if major != self.SCHEMA_VERSION_MAJOR:
            raise ValueError(
                f"ManifestParserV1 cannot parse manifest_schema_version {schema_version}. "
                f"Expected major version {self.SCHEMA_VERSION_MAJOR}."
            )
    
    # =========================================================================
    # IDENTITY EXTRACTION
    # =========================================================================
    
    def get_identity(self) -> Identity:
        """Extract identity block."""
        identity = self._manifest.get("identity", {})
        return Identity(
            name=identity.get("name", ""),
            domain=identity.get("domain", ""),
            owner_squad=identity.get("owner_squad", "")
        )
    
    def get_manifest_id(self) -> str:
        """Get the manifest identifier (name)."""
        return self.get_identity().name
    
    # =========================================================================
    # EVOLUTION EXTRACTION
    # =========================================================================
    
    def get_evolution(self) -> Evolution:
        """Extract evolution block with all version fields."""
        evolution = self._manifest.get("evolution", {})
        return Evolution(
            manifest_version=evolution.get("manifest_version", "1.0.0"),
            manifest_schema_version=evolution.get("manifest_schema_version", "1.0.0"),
            data_schema_version=evolution.get("data_schema_version", "1.0.0"),
            engine=evolution.get("engine", "python"),  # Default to python engine
            engine_version=evolution.get("engine_version", "1.0.0")
        )
    
    def get_manifest_version(self) -> str:
        """Get manifest version."""
        return self.get_evolution().manifest_version
    
    def get_manifest_schema_version(self) -> str:
        """Get manifest schema version."""
        return self.get_evolution().manifest_schema_version
    
    def get_data_schema_version(self) -> str:
        """Get data schema version."""
        return self.get_evolution().data_schema_version
    
    def get_engine(self) -> str:
        """Get engine type (python, python_spark, python_duckdb, etc.)."""
        return self.get_evolution().engine
    
    def get_engine_version(self) -> str:
        """Get engine version."""
        return self.get_evolution().engine_version
    
    # =========================================================================
    # AGENCY EXTRACTION (V1: from source_url)
    # =========================================================================
    
    def extract_agency(self) -> str:
        """
        Extract agency from manifest.
        
        V1 Schema: Agency is derived from intent.ingestion.params.source_url
        Example: "wild/bls/employment_stats.csv" -> "bls"
        """
        source_url = self.get_source_url()
        parts = source_url.replace("wild/", "").split("/")
        if len(parts) >= 1 and parts[0]:
            return parts[0]
        return "unknown"
    
    def get_source_url(self) -> str:
        """Get the source URL from ingestion params."""
        return (
            self._manifest.get("intent", {})
            .get("ingestion", {})
            .get("params", {})
            .get("source_url", "")
        )
    
    # =========================================================================
    # DATA MODEL RESOLUTION
    # =========================================================================
    
    def get_data_model_ref(self) -> Optional[Tuple[str, str]]:
        """
        Get data_model reference (path, version) from manifest.
        
        Returns:
            Tuple of (path, version) or None if not defined
        """
        data_model = self._manifest.get("data_model", {})
        path = data_model.get("path")
        version = data_model.get("version")
        
        if path and version:
            return (path, version)
        return None
    
    def resolve_data_model_path(self, layer: str = "curation") -> Optional[Path]:
        """
        Resolve full filesystem path to data_model file.
        
        V1 Schema: data_model files are deployed to:
        manifest_store/store/{layer}/manifests/{agency}/data_model/{path}.json
        
        Args:
            layer: The manifest layer (curation, semantics, retrieval)
            
        Returns:
            Resolved Path or None if data_model not defined
        """
        ref = self.get_data_model_ref()
        if not ref:
            return None
        
        path, version = ref
        agency = self.extract_agency()
        
        return (
            _PROJECT_ROOT / "mda_platform" / "control_plane" / "manifest_store" / "store"
            / layer / "manifests" / agency / "data_model" / f"{path}.json"
        )
    
    def load_data_model(self, layer: str = "curation") -> Optional[dict]:
        """
        Load and return data_model content.
        
        Args:
            layer: The manifest layer
            
        Returns:
            Parsed data_model dict or None
        """
        path = self.resolve_data_model_path(layer)
        if not path or not path.exists():
            return None
        
        with open(path, "r") as f:
            return json.load(f)
    
    # =========================================================================
    # REFERENCE DATA RESOLUTION
    # =========================================================================
    
    def get_reference_data_ref(self, ref_name: str) -> Optional[Tuple[str, str]]:
        """
        Get reference_data entry (path, version) by name.
        
        Args:
            ref_name: Reference data identifier (e.g., "state_mappings")
            
        Returns:
            Tuple of (path, version) or None if not found
        """
        ref_config = self._manifest.get("reference_data", {}).get(ref_name, {})
        path = ref_config.get("path")
        version = ref_config.get("version")
        
        if path and version:
            return (path, version)
        return None
    
    def list_reference_data(self) -> List[str]:
        """List all reference_data names defined in manifest."""
        return list(self._manifest.get("reference_data", {}).keys())
    
    def resolve_reference_data_path(
        self, ref_name: str, layer: str = "curation"
    ) -> Optional[Path]:
        """
        Resolve full filesystem path to reference_data file.
        
        V1 Schema: reference_data files are deployed to:
        manifest_store/store/{layer}/manifests/{agency}/reference_data/{path}_v{version}.json
        
        Args:
            ref_name: Reference data identifier
            layer: The manifest layer
            
        Returns:
            Resolved Path or None
        """
        ref = self.get_reference_data_ref(ref_name)
        if not ref:
            return None
        
        path, version = ref
        agency = self.extract_agency()
        
        return (
            _PROJECT_ROOT / "mda_platform" / "control_plane" / "manifest_store" / "store"
            / layer / "manifests" / agency / "reference_data" / f"{path}_v{version}.json"
        )
    
    def load_reference_data(self, ref_name: str, layer: str = "curation") -> Optional[dict]:
        """
        Load and return reference_data content.
        
        Args:
            ref_name: Reference data identifier
            layer: The manifest layer
            
        Returns:
            Parsed reference_data dict or None
        """
        path = self.resolve_reference_data_path(ref_name, layer)
        if not path or not path.exists():
            return None
        
        with open(path, "r") as f:
            return json.load(f)
    
    # =========================================================================
    # COMPONENT EXTRACTION
    # =========================================================================
    
    def get_ingestion_component(self) -> ComponentSpec:
        """
        Get the ingestion component specification.
        
        Returns:
            ComponentSpec with path, version, and params
        """
        ingest = self._manifest.get("intent", {}).get("ingestion", {})
        component = ingest.get("component", {})
        
        return ComponentSpec(
            path=component.get("path", ""),
            version=component.get("version", "1.0.0"),
            params=ingest.get("params", {})
        )
    
    def get_processing_steps(self) -> List[ProcessingStep]:
        """
        Get all processing steps from manifest.
        
        Returns:
            List of ProcessingStep objects in execution order
        """
        steps = []
        processing = self._manifest.get("intent", {}).get("processing", [])
        
        for step in processing:
            component = step.get("component", {})
            steps.append(ProcessingStep(
                step_name=step.get("step", ""),
                component=ComponentSpec(
                    path=component.get("path", ""),
                    version=component.get("version", "1.0.0"),
                    params=step.get("params", {})
                )
            ))
        
        return steps
    
    # =========================================================================
    # RAW MANIFEST ACCESS (Escape hatch for edge cases)
    # =========================================================================
    
    def get_raw_manifest(self) -> dict:
        """
        Get the raw manifest dict.
        
        Note: Prefer using typed accessors. Use this only when
        accessing non-standard extensions.
        """
        return self._manifest
