#!/usr/bin/env python3
# trigger.py
"""
MDA Orchestrator Trigger Script

Entry point for triggering manifest executions (Curation and Semantics layers).
Note: Retrieval layer uses retrieve.py (user-initiated, not orchestrator).

Usage:
    # Curation Layer (default)
    uv run trigger.py <agency>/<manifest_id>               # Run latest version
    uv run trigger.py <manifest_id> --version 1.0.0        # Specific version
    
    # Semantics Layer
    uv run trigger.py <manifest_id> --layer semantics      # Run semantic projection
    uv run trigger.py bls_employment_ontology --layer semantics
    
    # Utility
    uv run trigger.py --replay <utid>                      # Replay by UTID
    uv run trigger.py --status <utid>                      # Check execution status
    uv run trigger.py --list                               # List recent executions

Examples:
    uv run trigger.py census/census_population
    uv run trigger.py bls_employment_ontology --layer semantics
"""
import argparse
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from mda_platform.control_plane.orchestrator import trigger_curation_job, trigger_replay
from mda_platform.control_plane.manifest_store import ManifestStore
from mda_platform.control_plane.registry.parser_registry import get_parser
from mda_platform.execution_plane.common.connectors.evidence_store import EvidenceStore


def parse_manifest_input(input_str: str) -> tuple:
    """
    Parse manifest input which can be either:
    - 'census/census_population' (agency/manifest_id format, preferred)
    - 'census_population' (manifest_id only, legacy)
    
    Returns:
        Tuple of (agency, manifest_id) where agency may be None
    """
    if "/" in input_str:
        parts = input_str.split("/", 1)
        return (parts[0], parts[1])
    return (None, input_str)


def run_manifest(manifest_input: str, version: str = None, layer: str = "curation") -> dict:
    """Full execution flow: mint UTID, dispatch to engine, return result."""
    import importlib
    
    agency, manifest_id = parse_manifest_input(manifest_input)
    
    print(f"\n{'=' * 60}")
    print("MDA ORCHESTRATOR - TRIGGER")
    print(f"{'=' * 60}\n")
    
    # For semantics layer, we mint a new UTID directly (not through orchestrator's curation path)
    if layer == "semantics":
        return run_semantic_manifest(manifest_id, version)
    
    # Curation layer - standard orchestrator flow
    try:
        utid = trigger_curation_job(manifest_id, version)
    except ValueError as e:
        print(f"❌ TRIGGER FAILED: {e}")
        return {"status": "TRIGGER_FAILED", "error": str(e)}
    
    # Load manifest to determine engine type
    manifest = ManifestStore.get_manifest_for_execution(manifest_id, version)
    parser = get_parser(layer, manifest)
    engine = parser.get_engine()
    
    print(f"📤 DISPATCHING TO CURATION ENGINE ({engine})...")
    
    # Dynamic import of engine-specific interpreter
    interpreter_module = f"mda_platform.execution_plane.engines.curation_engine.{engine}.interpreter"
    try:
        engine_interpreter = importlib.import_module(interpreter_module)
    except ImportError as e:
        print(f"❌ ENGINE NOT FOUND: {interpreter_module}")
        return {"status": "ENGINE_NOT_FOUND", "error": str(e)}
    
    result = engine_interpreter.execute(utid, manifest_id, version)
    
    display_id = f"{agency}/{manifest_id}" if agency else manifest_id
    
    print(f"\n{'=' * 60}")
    print("EXECUTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  UTID: {utid}")
    print(f"  Manifest: {display_id}")
    print(f"  Status: {result['status']}")
    
    if result["status"] == "SUCCESS":
        print(f"  Components: {len(result['bom']['components_used'])}")
    else:
        print(f"  Error: {result.get('error', 'Unknown')}")
    
    print(f"\n  📋 Full evidence: evidence-store/{utid}.json")
    return result


def run_semantic_manifest(manifest_id: str, version: str = None) -> dict:
    """Execute a semantic manifest - Project-and-Translate workflow."""
    from mda_platform.control_plane.orchestrator.publisher import trigger_semantic_job
    
    # 1. ORCHESTRATOR: Mint UTID and enqueue (same pattern as curation)
    utid = trigger_semantic_job(manifest_id, version)
    
    # 2. Load manifest for execution
    manifest = ManifestStore.get_manifest_for_execution(manifest_id, version)
    parser = get_parser("semantics", manifest)
    
    print(f"📤 DISPATCHING TO SEMANTIC ENGINE...")
    
    # 3. Import and execute semantic engine
    try:
        from mda_platform.execution_plane.engines.semantic_engine import interpreter as semantic_interpreter
    except ImportError as e:
        print(f"❌ SEMANTIC ENGINE NOT FOUND: {e}")
        return {"status": "ENGINE_NOT_FOUND", "error": str(e)}
    
    result = semantic_interpreter.execute(manifest, utid)
    
    print(f"\n{'=' * 60}")
    print("EXECUTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"  UTID: {utid}")
    print(f"  Manifest: {manifest_id}")
    print(f"  Status: {result['status']}")
    
    if result["status"] == "SUCCESS":
        print(f"  Curation UTID: {result.get('curation_utid', 'N/A')}")
        print(f"  Records: {result.get('record_count', 0)}")
        print(f"  Components: {result.get('components', 0)}")
    else:
        print(f"  Error: {result.get('error', 'Unknown')}")
    
    print(f"\n  📋 Full evidence: evidence-store/{utid}.json")
    return result


def check_status(utid: str) -> None:
    """Display the status of an execution by UTID."""
    record = EvidenceStore.read_uir(utid)
    
    if not record:
        print(f"❌ No record found for UTID: {utid}")
        return
    
    print(f"\n{'=' * 60}")
    print(f"EXECUTION STATUS: {utid}")
    print(f"{'=' * 60}\n")
    
    print(f"  Status: {record.get('status', 'UNKNOWN')}")
    print(f"  Manifest: {record.get('manifest_id', 'N/A')}")
    print(f"  Version: {record.get('manifest_version', 'N/A')}")
    
    if "bom" in record:
        bom = record["bom"]
        print(f"\n  📦 Bill of Materials:")
        print(f"     Engine Version: {bom.get('engine_version', 'N/A')}")
        print(f"     Components Used: {len(bom.get('components_used', []))}")
        for comp in bom.get("components_used", []):
            print(f"       - [{comp['step']}] {comp['path']} (v{comp['version']})")


def list_executions() -> None:
    """List all executions in the Evidence Store."""
    records = EvidenceStore.list_all()
    
    print(f"\n{'=' * 60}")
    print("RECENT EXECUTIONS")
    print(f"{'=' * 60}\n")
    
    if not records:
        print("  (none)")
        return
    
    records.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    
    for r in records[:20]:
        status_icon = {
            "QUEUED": "🕐", "STARTED": "🔄", "SUCCESS": "✅", "FAILURE": "❌"
        }.get(r.get("status", ""), "❓")
        
        print(f"  {status_icon} {r.get('utid', 'N/A')[:40]}...")
        print(f"     Manifest: {r.get('manifest_id', 'N/A')} (v{r.get('manifest_version', '?')})")
        print(f"     Status: {r.get('status', 'UNKNOWN')}")
        print()


def replay_manifest(source_utid: str) -> dict:
    """Replay an execution using its historical Raw data."""
    print(f"\n{'=' * 60}")
    print("MDA ORCHESTRATOR - REPLAY")
    print(f"{'=' * 60}\n")
    
    original = EvidenceStore.read_uir(source_utid)
    if not original:
        print(f"❌ No record found for UTID: {source_utid}")
        return {"status": "FAILED", "error": f"UTID not found: {source_utid}"}
    
    manifest_id = original.get("manifest_id")
    manifest_version = original.get("manifest_version")
    
    print(f"📜 REPLAYING HISTORICAL EXECUTION")
    print(f"   Source UTID: {source_utid}")
    print(f"   Manifest: {manifest_id}")
    print(f"   Version: {manifest_version}")
    
    try:
        utid = trigger_replay(manifest_id, manifest_version, source_utid)
    except ValueError as e:
        print(f"❌ REPLAY TRIGGER FAILED: {e}")
        return {"status": "TRIGGER_FAILED", "error": str(e)}
    
    # Load manifest to determine engine type
    manifest = ManifestStore.get_manifest_for_execution(manifest_id, manifest_version)
    parser = get_parser(manifest)
    engine = parser.get_engine()
    
    print(f"\n📤 DISPATCHING TO CURATION ENGINE ({engine}) - REPLAY MODE...")
    
    # Dynamic import of engine-specific interpreter
    import importlib
    interpreter_module = f"mda_platform.execution_plane.engines.curation_engine.{engine}.interpreter"
    try:
        engine_interpreter = importlib.import_module(interpreter_module)
    except ImportError as e:
        print(f"❌ ENGINE NOT FOUND: {interpreter_module}")
        return {"status": "ENGINE_NOT_FOUND", "error": str(e)}
    
    result = engine_interpreter.execute(utid, manifest_id, manifest_version, source_utid=source_utid)
    
    print(f"\n{'=' * 60}")
    print("REPLAY SUMMARY")
    print(f"{'=' * 60}")
    print(f"  New UTID: {utid}")
    print(f"  Source UTID: {source_utid}")
    print(f"  Manifest: {manifest_id} (v{manifest_version})")
    print(f"  Status: {result['status']}")
    
    if result["status"] == "SUCCESS":
        print(f"\n  ⏮️  REPLAY: Used historical Raw data from {source_utid[:12]}...")
    
    return result


def main():
    parser = argparse.ArgumentParser(description="Trigger MDA manifest execution")
    parser.add_argument("manifest_id", nargs="?", help="The manifest ID to execute")
    parser.add_argument("--version", "-v", type=str, help="Specific manifest version")
    parser.add_argument("--layer", type=str, default="curation", 
                        choices=["curation", "semantics"],
                        help="Layer to execute (curation or semantics)")
    parser.add_argument("--replay", "-r", type=str, metavar="UTID", help="Replay by UTID")
    parser.add_argument("--replay-version", type=str, metavar="VERSION", help="Replay by version")
    parser.add_argument("--status", "-s", type=str, metavar="UTID", help="Check execution status")
    parser.add_argument("--list", "-l", action="store_true", help="List recent executions")
    
    args = parser.parse_args()
    
    if args.list:
        list_executions()
        return
    
    if args.status:
        check_status(args.status)
        return
    
    if args.replay:
        result = replay_manifest(args.replay)
        sys.exit(0 if result["status"] == "SUCCESS" else 1)
    
    if args.replay_version:
        if not args.manifest_id:
            print("❌ Error: --replay-version requires manifest_id")
            sys.exit(1)
        
        original = EvidenceStore.find_first_success(args.manifest_id, args.replay_version)
        if not original:
            print(f"❌ No successful execution found for {args.manifest_id} v{args.replay_version}")
            sys.exit(1)
        
        print(f"🔍 Found original execution: {original['utid']}")
        result = replay_manifest(original["utid"])
        sys.exit(0 if result["status"] == "SUCCESS" else 1)
    
    if not args.manifest_id:
        parser.print_help()
        print("\n❌ Error: manifest_id is required")
        sys.exit(1)
    
    result = run_manifest(args.manifest_id, args.version, args.layer)
    sys.exit(0 if result["status"] == "SUCCESS" else 1)


if __name__ == "__main__":
    main()
