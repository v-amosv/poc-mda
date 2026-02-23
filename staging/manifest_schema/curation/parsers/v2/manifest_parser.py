# mda_platform/control_plane/manifest_schema/v2/manifest_parser.py
"""
V2 Manifest Schema Parser

This parser understands manifest_schema_version 2.x.x structure.
V2 introduces YAML format support and a nested 'manifest' root key.

Key changes from V1:
  - YAML format (in addition to JSON)
  - Root 'manifest' wrapper key
  - identity.agency: explicit agency field (was derived from source_url in V1)
  - identity.owner: renamed from owner_squad
  - All content nested under 'manifest' key

V2 Schema Structure:
  manifest:
    identity:
      name: "agency/manifest_name"
      domain: "domain"
      agency: "agency"       # NEW: explicit agency
      owner: "owner"         # RENAMED: was owner_squad
    evolution:
      manifest_version: "x.y.z"
      manifest_schema_version: "2.x.x"
      data_schema_version: "x.y.z"
      engine: "python|python_spark|python_duckdb"
      engine_version: "x.y.z"
    reference_data:
      <ref_name>:
        path: "path"
        version: "x.y.z"
    data_model:
      path: "path"
      version: "x.y.z"
    intent:
      ingestion:
        component:
          path: "component.path"
          version: "x.y.z"
        params: {}
      processing:
        - step: "step_name"
          component:
            path: "component.path"
            version: "x.y.z"
          params: {}
    governance:
      retention_days: 730
      classification: "public|internal|confidential"
"""
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


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
    engine: str
    engine_version: str


@dataclass
class Identity:
    """Manifest identity block (V2: includes explicit agency)."""
    name: str
    domain: str
    agency: str  # NEW in V2: explicit agency field
    owner: str   # RENAMED in V2: was owner_squad


@dataclass
class Governance:
    """Manifest governance block."""
    retention_days: int
    classification: str


class ManifestParserV2:
    """
    V2 Manifest Parser - YAML-native with nested 'manifest' root.
    
    Key differences from V1:
    - Supports YAML format natively
    - Expects 'manifest' wrapper key
    - Agency is explicit in identity block (not derived)
    - owner_squad renamed to owner
    
    Usage:
        parser = ManifestParserV2(manifest)
        agency = parser.extract_agency()  # Now reads from identity.agency
        data_model_path = parser.get_data_model_path()
        steps = parser.get_processing_steps()
    """
    
    # Schema version this parser handles
    SCHEMA_VERSION_MAJOR = 2
    
    def __init__(self, manifest: dict):
        """
        Initialize parser with a manifest dict.
        
        Args:
            manifest: The manifest dictionary to parse (may have 'manifest' wrapper)
            
        Raises:
            ValueError: If manifest is incompatible with V2 parser
        """
        # V2 manifests have a 'manifest' wrapper key
        if "manifest" in manifest:
            self._manifest = manifest["manifest"]
        else:
            # Support unwrapped manifests for backwards compatibility
            self._manifest = manifest
            
        self._validate_schema_compatibility()
    
    def _validate_schema_compatibility(self) -> None:
        """Ensure this manifest is compatible with V2 parser."""
        schema_version = self._manifest.get("evolution", {}).get("manifest_schema_version", "2.0.0")
        major = int(schema_version.split(".")[0])
        
        if major != self.SCHEMA_VERSION_MAJOR:
            raise ValueError(
                f"ManifestParserV2 cannot parse manifest_schema_version {schema_version}. "
                f"Expected major version {self.SCHEMA_VERSION_MAJOR}."
            )
    
    # =========================================================================
    # IDENTITY EXTRACTION (V2: explicit agency)
    # =========================================================================
    
    def get_identity(self) -> Identity:
        """Extract identity block with V2 fields."""
        identity = self._manifest.get("identity", {})
        return Identity(
            name=identity.get("name", ""),
            domain=identity.get("domain", ""),
            agency=identity.get("agency", ""),  # V2: explicit field
            owner=identity.get("owner", "")     # V2: renamed from owner_squad
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
            manifest_version=evolution.get("manifest_version", "2.0.0"),
            manifest_schema_version=evolution.get("manifest_schema_version", "2.0.0"),
            data_schema_version=evolution.get("data_schema_version", "1.0.0"),
            engine=evolution.get("engine", "python"),
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
    # AGENCY EXTRACTION (V2: explicit in identity)
    # =========================================================================
    
    def extract_agency(self) -> str:
        """
        Extract agency from manifest.
        
        V2 Schema: Agency is explicit in identity.agency
        Falls back to V1 derivation from source_url if not present.
        """
        agency = self.get_identity().agency
        if agency:
            return agency
        
        # Fallback: derive from source_url (V1 compatibility)
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
    # GOVERNANCE (NEW in V2)
    # =========================================================================
    
    def get_governance(self) -> Governance:
        """Extract governance block (V2 feature)."""
        governance = self._manifest.get("governance", {})
        return Governance(
            retention_days=governance.get("retention_days", 365),
            classification=governance.get("classification", "internal")
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
    # RAW MANIFEST ACCESS
    # =========================================================================
    
    def get_raw_manifest(self) -> dict:
        """
        Get the raw manifest dict (without 'manifest' wrapper).
        
        Note: Prefer using typed accessors. Use this only when
        accessing non-standard extensions.
        """
        return self._manifest
    
    # =========================================================================
    # YAML FILE LOADING (V2 feature)
    # =========================================================================
    
    @classmethod
    def from_yaml_file(cls, path: Path) -> "ManifestParserV2":
        """
        Load a V2 manifest from a YAML file.
        
        Args:
            path: Path to the YAML manifest file
            
        Returns:
            ManifestParserV2 instance
        """
        with open(path, "r") as f:
            manifest = yaml.safe_load(f)
        return cls(manifest)
    
    @classmethod
    def from_json_file(cls, path: Path) -> "ManifestParserV2":
        """
        Load a V2 manifest from a JSON file.
        
        Args:
            path: Path to the JSON manifest file
            
        Returns:
            ManifestParserV2 instance
        """
        with open(path, "r") as f:
            manifest = json.load(f)
        return cls(manifest)
    
    @classmethod
    def from_file(cls, path: Path) -> "ManifestParserV2":
        """
        Load a V2 manifest from file (auto-detects format).
        
        Args:
            path: Path to manifest file (.yaml, .yml, or .json)
            
        Returns:
            ManifestParserV2 instance
        """
        suffix = path.suffix.lower()
        if suffix in (".yaml", ".yml"):
            return cls.from_yaml_file(path)
        else:
            return cls.from_json_file(path)
