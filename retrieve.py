#!/usr/bin/env python3
"""
retrieve.py - User-initiated Retrieval CLI

This is the entry point for RETRIEVAL operations - distinct from trigger.py
which handles orchestrator-triggered curation and semantics.

UTID Generation:
  - Mints utid-{uuid} for each retrieval request
  - Links back to all Semantic UTIDs consumed

Usage:
  uv run retrieve.py <manifest_id> [--output-format FORMAT] [--params KEY=VALUE...]

Examples:
  uv run retrieve.py employment_analysis
  uv run retrieve.py employment_analysis --output-format flat_csv
  uv run retrieve.py employment_analysis --params year=2024 state=CA
"""
import argparse
import importlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# Try YAML import
try:
    import yaml
except ImportError:
    yaml = None

# Import ManifestStore for deployed manifests
from mda_platform.control_plane.manifest_store import ManifestStore


def _load_manifest(manifest_id: str, version: str = None) -> tuple[dict, str]:
    """
    Load a retrieval manifest from the Manifest Store.
    
    Args:
        manifest_id: The manifest ID (e.g., 'employment_analysis')
        version: Optional specific version
        
    Returns:
        Tuple of (manifest_dict, version_string)
    """
    # Use ManifestStore to get the deployed manifest
    manifest = ManifestStore.get_manifest_for_execution(manifest_id, version)
    
    if not manifest:
        raise FileNotFoundError(
            f"Retrieval manifest '{manifest_id}' not deployed. Run deploy.py first."
        )
    
    version = manifest.get("evolution", {}).get("manifest_version", "1.0.0")
    return manifest, version


def _mint_retrieval_utid() -> str:
    """Generate a unique UTID for retrieval operations."""
    return f"utid-{uuid.uuid4()}"


def _run_retrieval(manifest: dict, utid: str, params: dict) -> dict:
    """
    Execute the retrieval workflow via the Retrieval Engine.
    
    The engine will:
    1. Load semantic projections from semantic_store
    2. Execute join/synthesis components
    3. Format output (JSON-LD, CSV, etc.)
    4. Write evidence to evidence_store
    """
    # Get engine info from manifest
    evolution = manifest.get("evolution", {})
    engine_name = evolution.get("engine", "retrieval_engine")
    
    # Import the engine interpreter
    engine_module_path = f"mda_platform.execution_plane.engines.{engine_name}.interpreter"
    
    try:
        engine_module = importlib.import_module(engine_module_path)
    except ImportError as e:
        raise RuntimeError(f"Failed to import retrieval engine '{engine_name}': {e}")
    
    # Execute the retrieval
    result = engine_module.execute(manifest, utid, params)
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="User-initiated Retrieval CLI",
        epilog="Example: uv run retrieve.py employment_analysis"
    )
    
    parser.add_argument(
        "manifest_id",
        help="The retrieval manifest ID (e.g., 'employment_analysis')"
    )
    
    parser.add_argument(
        "--output-format",
        choices=["jsonld", "flat_csv", "parquet", "json"],
        default=None,
        help="Override output format (default: from manifest)"
    )
    
    parser.add_argument(
        "--params",
        nargs="*",
        default=[],
        help="Additional parameters as KEY=VALUE pairs"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running"
    )
    
    args = parser.parse_args()
    
    # Parse params
    params = {}
    for p in args.params:
        if "=" in p:
            key, value = p.split("=", 1)
            params[key] = value
    
    # Override output format if specified
    if args.output_format:
        params["output_format"] = args.output_format
    
    # Load the manifest
    print(f"\n{'='*60}")
    print("RETRIEVE CLI - User-Initiated Retrieval")
    print(f"{'='*60}")
    print(f"  Manifest ID: {args.manifest_id}")
    
    try:
        manifest, version = _load_manifest(args.manifest_id)
        print(f"  Version: {version}")
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: {e}")
        sys.exit(1)
    
    # Mint UTID
    utid = _mint_retrieval_utid()
    print(f"  UTID: {utid}")
    print(f"  Timestamp: {datetime.now(timezone.utc).isoformat()}")
    
    if params:
        print(f"  Params: {params}")
    
    # Dry run check
    if args.dry_run:
        identity = manifest.get('identity', {})
        print(f"\n[DRY RUN] Would execute retrieval for manifest:")
        print(f"  Name: {identity.get('name')}")
        print(f"  Version: {version}")
        print(f"  Domain: {identity.get('domain')}")
        print(f"  Question: {manifest.get('intent', {}).get('question')}")
        sys.exit(0)
    
    # Execute retrieval
    print(f"\n{'─'*60}")
    print("EXECUTING RETRIEVAL...")
    print(f"{'─'*60}")
    
    try:
        result = _run_retrieval(manifest, utid, params)
        
        print(f"\n{'='*60}")
        print("✅ RETRIEVAL COMPLETE")
        print(f"{'='*60}")
        print(f"  UTID: {utid}")
        print(f"  Status: {result.get('status', 'unknown')}")
        print(f"  Output Path: {result.get('output_path', 'N/A')}")
        print(f"  Record Count: {result.get('record_count', 'N/A')}")
        print(f"  Evidence: {result.get('evidence_path', 'N/A')}")
        
        # Show semantic lineage
        semantic_utids = result.get("semantic_utids", [])
        if semantic_utids:
            print(f"\n  Semantic Lineage:")
            for sem_utid in semantic_utids:
                print(f"    ← {sem_utid}")
        
    except Exception as e:
        print(f"\n❌ RETRIEVAL FAILED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()