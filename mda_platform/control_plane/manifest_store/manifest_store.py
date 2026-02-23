# platform/control_plane/manifest_store/manifest_store.py
"""
Manifest Store Connector

The Manifest Store holds deployed manifests with FULL VERSION HISTORY.
Each version is stored immutably - the store is append-only.
Deployment is idempotent - only new versions get deployed.

Structure:
    manifest_store/store/
      {layer}/                      # curation, semantics, retrieval
        manifests/
          {agency}/                 # bls, census, etc.
            {manifest_id}/
              v{version}/
                manifest.json       # The deployed manifest
              _latest.json          # Pointer to latest version
      reference_data/
        {ref_type}/
          v{version}.json

For POC: JSON file-based storage
Production: Would be a database or versioned object store
"""
import json
import hashlib
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

# Resolve path relative to project root (mda-poc-v2)
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
MANIFEST_STORE_PATH = _PROJECT_ROOT / "mda_platform" / "control_plane" / "manifest_store" / "store"
REGISTRY_PATH = _PROJECT_ROOT / "mda_platform" / "control_plane" / "registry"


class ManifestStore:
    """
    Static methods for deploying and retrieving manifests.
    Maintains full version history (Museum of Code).
    """

    @staticmethod
    def _compute_hash(manifest: dict) -> str:
        """Compute SHA256 hash of manifest content."""
        # Normalize by sorting keys for consistent hashing
        content = json.dumps(manifest, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    @staticmethod
    def _compare_versions(v1: str, v2: str) -> int:
        """
        Compare two semantic version strings.
        
        Returns:
            -1 if v1 < v2, 0 if equal, 1 if v1 > v2
        """
        def parse_version(v: str) -> tuple:
            parts = v.split(".")
            return tuple(int(p) for p in parts if p.isdigit())
        
        p1, p2 = parse_version(v1), parse_version(v2)
        if p1 < p2:
            return -1
        elif p1 > p2:
            return 1
        return 0

    @staticmethod
    def _get_manifest_id(manifest: dict) -> str:
        """Extract manifest ID from identity block."""
        return manifest["identity"]["name"]

    @staticmethod
    def _get_manifest_dir(manifest_id: str, layer: str = None, agency: str = None) -> Path:
        """Get the directory for a manifest (contains all versions).
        
        If layer/agency provided, uses new structure:
            store/{layer}/manifests/{agency}/{manifest_id}/
        Otherwise falls back to flat structure for backward compat:
            store/{manifest_id}/
        """
        if layer and agency:
            return MANIFEST_STORE_PATH / layer / "manifests" / agency / manifest_id
        # Backward compatibility: search all layers/agencies
        return MANIFEST_STORE_PATH / manifest_id

    @staticmethod
    def _find_manifest_location(manifest_id: str) -> Optional[tuple]:
        """Find layer and agency for an existing manifest.
        
        Returns:
            Tuple of (layer, agency) or None if not found
        """
        for layer in ["curation", "semantics", "retrieval"]:
            layer_path = MANIFEST_STORE_PATH / layer / "manifests"
            if not layer_path.exists():
                continue
            for agency_dir in layer_path.iterdir():
                if agency_dir.is_dir():
                    manifest_dir = agency_dir / manifest_id
                    if manifest_dir.exists():
                        return (layer, agency_dir.name)
        return None

    @staticmethod
    def _get_version_path(manifest_id: str, version: str, layer: str = None, agency: str = None) -> Path:
        """Get the path for a specific manifest version."""
        manifest_dir = ManifestStore._get_manifest_dir(manifest_id, layer, agency)
        return manifest_dir / f"v{version}" / "manifest.json"

    @staticmethod
    def _get_latest_pointer_path(manifest_id: str, layer: str = None, agency: str = None) -> Path:
        """Get the path for the latest version pointer file."""
        manifest_dir = ManifestStore._get_manifest_dir(manifest_id, layer, agency)
        return manifest_dir / "_latest.json"

    @staticmethod
    def get_deployed(manifest_id: str, version: str = None) -> Optional[dict]:
        """
        Get a deployed manifest by ID and optionally version.
        
        Args:
            manifest_id: The manifest identifier (e.g., 'bls_employment_stats')
            version: Optional specific version (defaults to latest)
            
        Returns:
            The deployed manifest record or None
        """
        # Find the manifest location (layer/agency)
        location = ManifestStore._find_manifest_location(manifest_id)
        layer, agency = location if location else (None, None)
        
        if version:
            # Get specific version
            version_path = ManifestStore._get_version_path(manifest_id, version, layer, agency)
            if not version_path.exists():
                return None
            with open(version_path, "r") as f:
                return json.load(f)
        else:
            # Get latest version
            latest_path = ManifestStore._get_latest_pointer_path(manifest_id, layer, agency)
            if not latest_path.exists():
                return None
            
            with open(latest_path, "r") as f:
                pointer = json.load(f)
            
            return ManifestStore.get_deployed(manifest_id, pointer["version"])

    @staticmethod
    def get_deployed_version(manifest_id: str) -> Optional[str]:
        """Get the latest deployed version for a manifest."""
        location = ManifestStore._find_manifest_location(manifest_id)
        layer, agency = location if location else (None, None)
        
        latest_path = ManifestStore._get_latest_pointer_path(manifest_id, layer, agency)
        if not latest_path.exists():
            return None
        
        with open(latest_path, "r") as f:
            pointer = json.load(f)
        return pointer["version"]

    @staticmethod
    def get_all_versions(manifest_id: str) -> List[str]:
        """
        Get all deployed versions for a manifest (sorted oldest to newest).
        
        Args:
            manifest_id: The manifest identifier
            
        Returns:
            List of version strings, e.g., ["1.0.0", "1.1.0", "1.2.0"]
        """
        location = ManifestStore._find_manifest_location(manifest_id)
        layer, agency = location if location else (None, None)
        
        manifest_dir = ManifestStore._get_manifest_dir(manifest_id, layer, agency)
        if not manifest_dir.exists():
            return []
        
        versions = []
        for version_dir in manifest_dir.iterdir():
            if version_dir.is_dir() and version_dir.name.startswith("v"):
                version = version_dir.name[1:]  # Strip 'v' prefix
                versions.append(version)
        
        # Sort by semantic version
        def version_key(v: str) -> tuple:
            parts = v.split(".")
            return tuple(int(p) for p in parts if p.isdigit())
        
        return sorted(versions, key=version_key)

    @staticmethod
    def deploy(manifest: dict, force: bool = False, layer: str = None, agency: str = None) -> dict:
        """
        Deploy a manifest to the store.
        
        Stores each version immutably - the store maintains full history.
        
        Args:
            manifest: The manifest dict to deploy (or path to JSON file)
            force: If True, skip hash validation (not recommended for production)
            layer: The layer (curation, semantics, retrieval)
            agency: The agency (bls, census, etc.)
            
        Returns:
            Deployment result with status
            
        Raises:
            ValueError: If manifest_version unchanged but content differs (hash mismatch)
        """
        # Handle string path input (JSON or YAML)
        if isinstance(manifest, str):
            filepath = Path(manifest)
            with open(filepath, "r") as f:
                if filepath.suffix == ".yaml":
                    data = yaml.safe_load(f)
                    # V2 YAML manifests have 'manifest:' wrapper
                    manifest = data.get("manifest", data)
                else:
                    manifest = json.load(f)
        
        MANIFEST_STORE_PATH.mkdir(parents=True, exist_ok=True)
        
        manifest_id = ManifestStore._get_manifest_id(manifest)
        new_version = manifest["evolution"]["manifest_version"]
        new_hash = ManifestStore._compute_hash(manifest)
        
        # Check if this specific version already exists
        existing_version_record = ManifestStore.get_deployed(manifest_id, new_version)
        
        if existing_version_record:
            existing_hash = existing_version_record["content_hash"]
            
            if existing_hash == new_hash:
                # Exact same content - skip
                return {
                    "status": "SKIPPED",
                    "reason": "already_deployed",
                    "manifest_id": manifest_id,
                    "version": new_version
                }
            else:
                # CRITICAL: Same version but different content
                if not force:
                    raise ValueError(
                        f"GOVERNANCE VIOLATION: Manifest '{manifest_id}' v{new_version} "
                        f"already deployed with different content. "
                        f"Hash mismatch: {existing_hash} != {new_hash}. "
                        "Increment manifest_version to deploy changes."
                    )
        
        # Get previous latest version
        previous_version = ManifestStore.get_deployed_version(manifest_id)
        
        # Deploy new version
        record = {
            "manifest_id": manifest_id,
            "version": new_version,
            "manifest": manifest,
            "content_hash": new_hash,
            "deployed_at": datetime.now(timezone.utc).isoformat(),
            "previous_version": previous_version,
            "layer": layer,
            "agency": agency
        }
        
        # Create version directory and save (using layer/agency structure if provided)
        manifest_dir = ManifestStore._get_manifest_dir(manifest_id, layer, agency)
        version_dir = manifest_dir / f"v{new_version}"
        version_dir.mkdir(parents=True, exist_ok=True)
        
        version_path = version_dir / "manifest.json"
        with open(version_path, "w") as f:
            json.dump(record, f, indent=2)
        
        # Update latest pointer ONLY if new version is higher than current latest
        current_latest = ManifestStore.get_deployed_version(manifest_id)
        should_update_latest = (
            current_latest is None or 
            ManifestStore._compare_versions(new_version, current_latest) > 0
        )
        
        if should_update_latest:
            latest_pointer = {
                "version": new_version,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            latest_path = ManifestStore._get_latest_pointer_path(manifest_id, layer, agency)
            with open(latest_path, "w") as f:
                json.dump(latest_pointer, f, indent=2)
        
        # Get engine info from manifest
        engine = manifest.get("evolution", {}).get("engine", "unknown")
        engine_version = manifest.get("evolution", {}).get("engine_version", "unknown")
        
        return {
            "status": "DEPLOYED",
            "manifest_id": manifest_id,
            "version": new_version,
            "content_hash": new_hash,
            "previous_version": previous_version,
            "is_latest": should_update_latest,
            "engine": engine,
            "engine_version": engine_version
        }

    @staticmethod
    def get_manifest_for_execution(manifest_id: str, version: str = None) -> dict:
        """
        Get a manifest ready for execution.
        
        Args:
            manifest_id: The manifest identifier
            version: Optional specific version (defaults to latest deployed)
            
        Returns:
            The manifest dict
            
        Raises:
            ValueError: If manifest not found
        """
        deployed = ManifestStore.get_deployed(manifest_id, version)
        
        if not deployed:
            if version:
                raise ValueError(
                    f"Manifest '{manifest_id}' v{version} not found. "
                    f"Available versions: {ManifestStore.get_all_versions(manifest_id)}"
                )
            else:
                raise ValueError(f"Manifest '{manifest_id}' not deployed. Run deploy.py first.")
        
        return deployed["manifest"]

    @staticmethod
    def list_deployed() -> List[Dict]:
        """List all deployed manifests with summary info (latest versions)."""
        MANIFEST_STORE_PATH.mkdir(parents=True, exist_ok=True)
        summaries = []
        
        for manifest_dir in MANIFEST_STORE_PATH.iterdir():
            if not manifest_dir.is_dir():
                continue
            if manifest_dir.name.startswith("_") or manifest_dir.name == "reference_data":
                continue
            
            manifest_id = manifest_dir.name
            latest_record = ManifestStore.get_deployed(manifest_id)
            
            if latest_record:
                all_versions = ManifestStore.get_all_versions(manifest_id)
                summaries.append({
                    "manifest_id": manifest_id,
                    "version": latest_record["version"],
                    "deployed_at": latest_record["deployed_at"],
                    "content_hash": latest_record["content_hash"],
                    "version_count": len(all_versions),
                    "all_versions": all_versions
                })
        
        return summaries

    @staticmethod
    def list_versions(manifest_id: str) -> List[Dict]:
        """
        List all versions of a specific manifest with details.
        
        Args:
            manifest_id: The manifest identifier
            
        Returns:
            List of version records with metadata
        """
        versions = ManifestStore.get_all_versions(manifest_id)
        records = []
        
        for version in versions:
            record = ManifestStore.get_deployed(manifest_id, version)
            if record:
                records.append({
                    "version": version,
                    "deployed_at": record["deployed_at"],
                    "content_hash": record["content_hash"],
                    "previous_version": record.get("previous_version")
                })
        
        return records
