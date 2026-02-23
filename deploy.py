#!/usr/bin/env python3
# deploy.py
"""
MDA Manifest Deployment Script

Deploys manifest from Registry to Manifest Store.
Also copies data_model and reference_data folders.

Lifecycle:
  1. Staging (Development)       - Manual edits
  2. Registry (PR Ready)         - onboard.py
  3. Manifest Store (Deployed)   - deploy.py  ← THIS STEP
  4. Execution                   - trigger.py

Usage:
    uv run deploy.py <layer>/<agency>/<manifest_name>
    uv run deploy.py --list-registry

Examples:
    uv run deploy.py curation/bls/bls_employment_stats_v1.0.0
    uv run deploy.py curation/census/census_population_v1.1.0
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).parent
REGISTRY_ROOT = PROJECT_ROOT / "mda_platform" / "control_plane" / "registry"
MANIFEST_STORE_ROOT = PROJECT_ROOT / "mda_platform" / "control_plane" / "manifest_store" / "store"

sys.path.insert(0, str(PROJECT_ROOT))

from mda_platform.execution_plane.common.connectors.evidence_store import EvidenceStore


def list_registry() -> None:
    """List all manifests in registry (ready for deployment)."""
    print(f"\n{'=' * 60}")
    print("REGISTRY - Ready for Deployment")
    print(f"{'=' * 60}\n")
    
    layers = ["curation", "semantics", "retrieval"]
    found_any = False
    
    for layer in layers:
        layer_path = REGISTRY_ROOT / layer / "manifests"
        if not layer_path.exists():
            continue
            
        for agency_dir in sorted(layer_path.iterdir()):
            if not agency_dir.is_dir():
                continue
            # Find both JSON and YAML manifests
            manifests = sorted(list(agency_dir.glob("*.json")) + list(agency_dir.glob("*.yaml")))
            if manifests:
                found_any = True
                print(f"  📂 {layer}/{agency_dir.name}/")
                for m in manifests:
                    data = _load_manifest_file(m)
                    name = data.get("identity", {}).get("name", "?")
                    version = data.get("evolution", {}).get("manifest_version", "?")
                    print(f"     📄 {m.stem}")
                    print(f"        Name: {name}, Version: {version}")
                print()
    
    if not found_any:
        print("  (empty)")


def _load_manifest_file(filepath: Path) -> dict:
    """
    Load manifest from JSON or YAML file.
    
    For YAML files with 'manifest:' wrapper (V2 schema), unwrap it.
    """
    with open(filepath, "r") as f:
        if filepath.suffix == ".yaml":
            data = yaml.safe_load(f)
            # V2 YAML manifests have 'manifest:' wrapper
            if "manifest" in data:
                return data["manifest"]
            return data
        else:
            return json.load(f)


def _find_registry_file(registry_agency_dir: Path, manifest_name: str) -> Path:
    """
    Find manifest file in registry (JSON or YAML).
    """
    json_file = registry_agency_dir / f"{manifest_name}.json"
    yaml_file = registry_agency_dir / f"{manifest_name}.yaml"
    
    if json_file.exists():
        return json_file
    elif yaml_file.exists():
        return yaml_file
    else:
        raise FileNotFoundError(
            f"Manifest not found: {manifest_name}.json or {manifest_name}.yaml"
        )


def deploy(manifest_path: str) -> bool:
    """
    Deploy manifest: Registry → Manifest Store
    Also copies data_model and reference_data folders.
    
    Args:
        manifest_path: Path like "curation/bls/bls_employment_stats_v1.0.0"
        
    Returns:
        Success status
    """
    print(f"\n{'=' * 60}")
    print("MDA MANIFEST DEPLOYMENT (Registry → Manifest Store)")
    print(f"{'=' * 60}\n")
    
    # Parse path
    parts = manifest_path.split("/")
    if len(parts) != 3:
        print(f"   ❌ Invalid path format. Expected: layer/agency/manifest_name")
        print(f"      Example: curation/bls/bls_employment_stats_v1.0.0")
        return False
    
    layer, agency, manifest_name = parts
    
    # Find in registry (JSON or YAML)
    registry_agency_dir = REGISTRY_ROOT / layer / "manifests" / agency
    try:
        registry_file = _find_registry_file(registry_agency_dir, manifest_name)
    except FileNotFoundError as e:
        print(f"   ❌ {e}")
        print(f"   Run: uv run onboard.py {manifest_path}")
        return False
    
    print(f"📦 Registry → Manifest Store")
    print(f"   Source: {registry_file.relative_to(PROJECT_ROOT)}")
    
    from mda_platform.control_plane.manifest_store import ManifestStore
    
    try:
        result = ManifestStore.deploy(str(registry_file), layer=layer, agency=agency)
        if result.get("status") == "SKIPPED":
            print(f"   ⏭️  Skipped: {result['manifest_id']} v{result['version']} (already deployed)")
        else:
            print(f"   ✅ Deployed: {result['manifest_id']} v{result['version']}")
            print(f"   Content Hash: {result['content_hash']}")
            
            # Record deployment evidence
            deployment_id = EvidenceStore.write_deployment(
                manifest_id=result['manifest_id'],
                manifest_version=result['version'],
                content_hash=result['content_hash'],
                layer=layer,
                agency=agency,
                engine=result.get('engine'),
                engine_version=result.get('engine_version'),
                source_path=str(registry_file.relative_to(PROJECT_ROOT)),
                target_path=f"mda_mda_platform/control_plane/manifest_store/store/{layer}/manifests/{agency}"
            )
            print(f"   📋 Evidence: {deployment_id[:20]}...")
    except Exception as e:
        print(f"   ❌ Deployment failed: {e}")
        return False
    
    # Copy data_model folder if exists in registry
    registry_data_model = registry_agency_dir / "data_model"
    if registry_data_model.exists():
        store_data_model = MANIFEST_STORE_ROOT / layer / "manifests" / agency / "data_model"
        if store_data_model.exists():
            shutil.rmtree(store_data_model)
        shutil.copytree(registry_data_model, store_data_model)
        print(f"   📂 Copied data_model/ to Manifest Store")
    
    # Copy reference_data folder if exists in registry
    registry_ref_data = registry_agency_dir / "reference_data"
    if registry_ref_data.exists():
        store_ref_data = MANIFEST_STORE_ROOT / layer / "manifests" / agency / "reference_data"
        if store_ref_data.exists():
            shutil.rmtree(store_ref_data)
        shutil.copytree(registry_ref_data, store_ref_data)
        print(f"   📂 Copied reference_data/ to Manifest Store")
    
    print(f"\n{'=' * 60}")
    print("✅ DEPLOYMENT COMPLETE")
    print(f"   Manifest: {result['manifest_id']}")
    print(f"   Version: {result['version']}")
    print(f"   Layer: {layer}")
    print(f"   Agency: {agency}")
    print(f"   Ready to trigger: uv run trigger.py {result['manifest_id']}")
    print(f"{'=' * 60}\n")
    
    return True


def main():
    parser = argparse.ArgumentParser(description="MDA Manifest Deployment (Registry → Manifest Store)")
    parser.add_argument("manifest_path", nargs="?", help="Path: layer/agency/manifest_name")
    parser.add_argument("--list-registry", action="store_true", help="List registry manifests")
    
    args = parser.parse_args()
    
    if args.list_registry:
        list_registry()
        return
    
    if not args.manifest_path:
        parser.print_help()
        print("\n❌ Error: manifest_path is required")
        print("\nFormat: layer/agency/manifest_name")
        print("\nExamples:")
        print("  uv run deploy.py curation/bls/bls_employment_stats_v1.0.0")
        print("  uv run deploy.py curation/census/census_population_v1.1.0")
        print("  uv run deploy.py --list-registry")
        sys.exit(1)
    
    success = deploy(args.manifest_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
