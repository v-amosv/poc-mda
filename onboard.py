#!/usr/bin/env python3
# onboard.py
"""
MDA Manifest Onboarding Script

Copies manifest from Staging to Registry (PR Ready state).

Lifecycle:
  1. Staging (Development)       - Manual edits
  2. Registry (PR Ready)         - onboard.py  ← THIS STEP
  3. Manifest Store (Deployed)   - deploy.py
  4. Execution                   - trigger.py

Usage:
    uv run onboard.py <layer>/<agency>/<manifest_name>
    uv run onboard.py --list-staging
    uv run onboard.py --list-registry

Examples:
    uv run onboard.py curation/bls/bls_employment_stats_v1.0.0
    uv run onboard.py curation/census/census_population_v1.1.0
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).parent
STAGING_ROOT = PROJECT_ROOT / "staging"
REGISTRY_ROOT = PROJECT_ROOT / "mda_platform" / "control_plane" / "registry"
STORAGE_PLANE = PROJECT_ROOT / "mda_platform" / "storage_plane"

sys.path.insert(0, str(PROJECT_ROOT))
# Note: We don't import get_parser at module level because onboard.py 
# must work BEFORE any parsers are onboarded. Parsers are loaded lazily.


def list_staging() -> None:
    """List all manifests in staging by layer and agency."""
    print(f"\n{'=' * 60}")
    print("STAGING ZONE - Available Manifests")
    print(f"{'=' * 60}\n")
    
    layers = ["curation", "semantics", "retrieval"]
    found_any = False
    
    for layer in layers:
        layer_path = STAGING_ROOT / layer / "manifests"
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
                    engine = data.get("evolution", {}).get("engine_version", "?")
                    print(f"     📄 {m.stem}")
                    print(f"        Name: {name}, Version: {version}, Engine: {engine}")
                print()
    
    if not found_any:
        print("  (empty)")


def list_registry() -> None:
    """List all manifests in registry (PR ready) by layer and agency."""
    print(f"\n{'=' * 60}")
    print("REGISTRY - PR Ready Manifests")
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
                    print(f"     ✅ {m.stem}")
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


def _get_major_version(manifest: dict) -> int:
    """
    Extract major version from manifest_schema_version.
    
    Args:
        manifest: Manifest dict (unwrapped)
        
    Returns:
        Major version number (e.g., 2 for "2.0.0")
    """
    schema_version = manifest.get("evolution", {}).get("manifest_schema_version", "1.0.0")
    return int(schema_version.split(".")[0])


def _onboard_parser_registry_if_needed() -> bool:
    """
    Copy parser_registry.py from staging to registry root if not present.
    
    Returns:
        True if registry was copied, False if already exists
    """
    target_file = REGISTRY_ROOT / "parser_registry.py"
    
    if target_file.exists():
        return False  # Already onboarded
    
    source_file = STAGING_ROOT / "manifest_schema" / "parser_registry.py"
    
    if not source_file.exists():
        print(f"   ⚠️  No parser_registry.py found in staging")
        return False
    
    shutil.copy(source_file, target_file)
    return True


def _onboard_schema_if_needed(layer: str, major_version: int) -> bool:
    """
    Copy manifest schema from staging to registry/{layer}/schema/ if not present.
    
    Args:
        layer: Layer name (curation, semantics, retrieval)
        major_version: Major version number (1, 2, etc.)
        
    Returns:
        True if schema was copied, False if already exists
    """
    # Check if schema already exists in registry
    schema_dir = REGISTRY_ROOT / layer / "schema"
    schema_pattern = f"manifest_schema_v{major_version}.*.json"
    existing = list(schema_dir.glob(schema_pattern))
    
    if existing:
        return False  # Already onboarded
    
    # Find schema in staging
    staging_schema_dir = STAGING_ROOT / "manifest_schema" / layer
    staging_schemas = list(staging_schema_dir.glob(schema_pattern))
    
    if not staging_schemas:
        print(f"   ⚠️  No schema found in staging for {layer} v{major_version}")
        return False
    
    # Copy schema to registry
    schema_dir.mkdir(parents=True, exist_ok=True)
    for schema_file in staging_schemas:
        shutil.copy(schema_file, schema_dir / schema_file.name)
    
    return True


def _onboard_parser_if_needed(layer: str, major_version: int) -> bool:
    """
    Copy manifest parser from staging to registry/{layer}/schema/parsers/v{n}/.
    
    New architecture: parsers live under each layer's schema directory.
    
    Args:
        layer: Layer name (curation, semantics, retrieval)
        major_version: Major version number (1, 2, etc.)
        
    Returns:
        True if parser was copied, False if already exists
    """
    # Check if parser already exists in registry/{layer}/schema/parsers/v{n}/
    parser_dir = REGISTRY_ROOT / layer / "schema" / "parsers" / f"v{major_version}"
    
    if parser_dir.exists() and (parser_dir / "manifest_parser.py").exists():
        return False  # Already onboarded
    
    # Find parser in staging/manifest_schema/{layer}/parsers/v{n}/
    staging_parser_dir = STAGING_ROOT / "manifest_schema" / layer / "parsers" / f"v{major_version}"
    
    if not staging_parser_dir.exists():
        print(f"   ⚠️  No parser found in staging for {layer} v{major_version}")
        return False
    
    # Create parser directory and necessary __init__.py files for imports
    parser_dir.mkdir(parents=True, exist_ok=True)
    
    # Create __init__.py files at each level for Python imports
    schema_dir = REGISTRY_ROOT / layer / "schema"
    parsers_dir = schema_dir / "parsers"
    
    # schema/__init__.py
    schema_init = schema_dir / "__init__.py"
    if not schema_init.exists():
        schema_init.write_text(f'# {layer}/schema module\n"""Schema and parsers for {layer} manifests."""\n')
    
    # schema/parsers/__init__.py
    parsers_init = parsers_dir / "__init__.py"
    if not parsers_init.exists():
        parsers_init.write_text(f'# {layer}/schema/parsers module\n"""Versioned manifest parsers for {layer}."""\n')
    
    # Copy parser files
    for py_file in staging_parser_dir.glob("*.py"):
        shutil.copy(py_file, parser_dir / py_file.name)
    
    return True


def _find_staging_file(staging_agency_dir: Path, manifest_name: str) -> Path:
    """
    Find manifest file in staging (JSON or YAML).
    """
    json_file = staging_agency_dir / f"{manifest_name}.json"
    yaml_file = staging_agency_dir / f"{manifest_name}.yaml"
    
    if json_file.exists():
        return json_file
    elif yaml_file.exists():
        return yaml_file
    else:
        raise FileNotFoundError(
            f"Manifest not found in staging: {manifest_name}.json or {manifest_name}.yaml"
        )


def stage_to_registry(manifest_path: str) -> dict:
    """
    Copy manifest from Staging to Registry.
    Also copies data_model, reference_data folders, dataset to Wild if defined,
    and schema/parser if this is the first manifest of that major version.
    
    Args:
        manifest_path: Path like "curation/bls/bls_employment_stats_v1.0.0"
        
    Returns:
        Dict with onboarding results
    """
    # Parse path: layer/agency/manifest_name
    parts = manifest_path.split("/")
    if len(parts) != 3:
        raise ValueError(f"Invalid path format. Expected: layer/agency/manifest_name")
    
    layer, agency, manifest_name = parts
    
    # Find in staging (JSON or YAML)
    staging_agency_dir = STAGING_ROOT / layer / "manifests" / agency
    staging_file = _find_staging_file(staging_agency_dir, manifest_name)
    
    # Load manifest to check for dataset and schema version
    manifest = _load_manifest_file(staging_file)
    major_version = _get_major_version(manifest)
    
    # Result tracking
    result = {
        "registry_file": None,
        "dataset_copied": False,
        "schema_onboarded": False,
        "parser_onboarded": False,
        "parser_registry_onboarded": False,
        "major_version": major_version,
        "layer": layer,
    }
    
    # Onboard parser_registry.py if needed (first manifest ever)
    result["parser_registry_onboarded"] = _onboard_parser_registry_if_needed()
    
    # Onboard schema and parser if needed (first manifest of this major version for this layer)
    result["parser_onboarded"] = _onboard_parser_if_needed(layer, major_version)
    result["schema_onboarded"] = _onboard_schema_if_needed(layer, major_version)
    
    # Copy to registry (preserve original format: JSON or YAML)
    registry_dir = REGISTRY_ROOT / layer / "manifests" / agency
    registry_dir.mkdir(parents=True, exist_ok=True)
    registry_file = registry_dir / staging_file.name
    shutil.copy(staging_file, registry_file)
    result["registry_file"] = registry_file
    
    # Copy data_model folder if exists
    staging_data_model = staging_agency_dir / "data_model"
    if staging_data_model.exists():
        registry_data_model = registry_dir / "data_model"
        if registry_data_model.exists():
            shutil.rmtree(registry_data_model)
        shutil.copytree(staging_data_model, registry_data_model)
    
    # Copy reference_data folder if exists
    staging_ref_data = staging_agency_dir / "reference_data"
    if staging_ref_data.exists():
        registry_ref_data = registry_dir / "reference_data"
        if registry_ref_data.exists():
            shutil.rmtree(registry_ref_data)
        shutil.copytree(staging_ref_data, registry_ref_data)
    
    # Copy dataset to Wild if defined in manifest
    dataset_config = manifest.get("dataset", {})
    if dataset_config:
        dataset_path = dataset_config.get("path")
        target_path = dataset_config.get("target")
        
        if dataset_path and target_path:
            source_dataset = staging_agency_dir / dataset_path
            target_dataset = STORAGE_PLANE / target_path
            
            if source_dataset.exists():
                target_dataset.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source_dataset, target_dataset)
                result["dataset_copied"] = True
    
    return result


def onboard(manifest_path: str) -> bool:
    """
    Onboard manifest: Staging → Registry
    
    Args:
        manifest_path: Path like "curation/bls/bls_employment_stats_v1.0.0"
        
    Returns:
        Success status
    """
    print(f"\n{'=' * 60}")
    print("MDA MANIFEST ONBOARDING (Staging → Registry)")
    print(f"{'=' * 60}\n")
    
    # Parse path
    parts = manifest_path.split("/")
    if len(parts) != 3:
        print(f"   ❌ Invalid path format. Expected: layer/agency/manifest_name")
        print(f"      Example: curation/bls/bls_employment_stats_v1.0.0")
        return False
    
    layer, agency, manifest_name = parts
    
    # Staging → Registry
    print(f"📋 Staging → Registry")
    print(f"   Source: staging/{layer}/manifests/{agency}/{manifest_name}.[json|yaml]")
    
    try:
        result = stage_to_registry(manifest_path)
        
        # Report parser_registry onboarding (first manifest ever)
        if result["parser_registry_onboarded"]:
            print(f"   🏗️  Onboarded parser_registry.py (first manifest onboarded)")
        
        # Report schema/parser onboarding (first manifest of this major version for this layer)
        if result["parser_onboarded"]:
            print(f"   🔧 Onboarded manifest parser v{result['major_version']} for {layer}")
        if result["schema_onboarded"]:
            print(f"   📐 Onboarded manifest schema v{result['major_version']}.0.0 for {layer}")
        
        print(f"   ✅ Copied to registry: {result['registry_file'].relative_to(PROJECT_ROOT)}")
        if result["dataset_copied"]:
            print(f"   📂 Copied dataset to Wild (simulating source data change)")
    except FileNotFoundError as e:
        print(f"   ❌ {e}")
        return False
    
    print(f"\n{'=' * 60}")
    print("✅ ONBOARDING COMPLETE")
    print(f"   Manifest: {manifest_name}")
    print(f"   Layer: {layer}")
    print(f"   Agency: {agency}")
    print(f"   Schema Version: v{result['major_version']}.x.x")
    print(f"   Next step: uv run deploy.py {layer}/{agency}/{manifest_name}")
    print(f"{'=' * 60}\n")
    
    return True


def main():
    parser = argparse.ArgumentParser(description="MDA Manifest Onboarding (Staging → Registry)")
    parser.add_argument("manifest_path", nargs="?", help="Path: layer/agency/manifest_name")
    parser.add_argument("--list-staging", action="store_true", help="List staging manifests")
    parser.add_argument("--list-registry", action="store_true", help="List registry manifests")
    
    args = parser.parse_args()
    
    if args.list_staging:
        list_staging()
        return
    
    if args.list_registry:
        list_registry()
        return
    
    if not args.manifest_path:
        parser.print_help()
        print("\n❌ Error: manifest_path is required")
        print("\nFormat: layer/agency/manifest_name")
        print("\nExamples:")
        print("  uv run onboard.py curation/bls/bls_employment_stats_v1.0.0")
        print("  uv run onboard.py curation/census/census_population_v1.1.0")
        print("  uv run onboard.py --list-staging")
        sys.exit(1)
    
    success = onboard(args.manifest_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
