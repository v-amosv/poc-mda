#!/usr/bin/env python3
# mda_platform/execution_plane/engines/retrieval_engine/interpreter.py
"""
Retrieval Engine Interpreter

The Retrieval Engine is the "Knowledge Synthesizer." It takes multiple
Semantic Projections and joins them to answer specific questions.

This engine is USER-INITIATED (via retrieve.py), not orchestrator-triggered.

Workflow: Hydrate → Fetch Dependencies → Cross-Domain Join → Format → Evidence

Key Concepts:
- Fan-In pattern: Collect from multiple semantic sources
- UTID Chaining: Creates "Grandmother-Parent-Child" lineage
- Disposable outputs: Retrieval results can be regenerated
"""
import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

# Resolve project root
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from mda_platform.execution_plane.common.connectors.evidence_store import EvidenceStore

# Storage paths
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"
_SEMANTIC_STORE = _STORAGE_PLANE / "semantic_store"
_RETRIEVAL_STORE = _STORAGE_PLANE / "retrieval_store"


def _find_latest_semantic_for_manifest(manifest_ref: str) -> dict:
    """
    Find the latest semantic record for a given manifest reference.
    
    Args:
        manifest_ref: e.g., "bls_employment_ontology"
        
    Returns:
        Dict with semantic data and metadata
    """
    # Search all domain folders in semantic_store
    for domain_dir in _SEMANTIC_STORE.iterdir():
        if not domain_dir.is_dir():
            continue
        for sem_file in sorted(domain_dir.glob("semantic-*.json"), reverse=True):
            with open(sem_file) as f:
                sem = json.load(f)
            if manifest_ref in sem.get("metadata", {}).get("manifest_id", ""):
                return {
                    "path": str(sem_file),
                    "metadata": sem["metadata"],
                    "context": sem.get("context", {}),
                    "data": sem["data"]
                }
    
    raise ValueError(f"No semantic projection found for: {manifest_ref}")


def execute(manifest: dict, utid: str, params: dict = None) -> dict:
    """
    Execute Retrieval Engine workflow.
    
    Args:
        manifest: The retrieval manifest dict (already unwrapped from V2 wrapper)
        utid: Retrieval UTID (minted by retrieve.py)
        params: Optional runtime parameters from CLI
        
    Returns:
        Execution result dict
    """
    params = params or {}
    
    # Extract manifest metadata (V2 format)
    identity = manifest.get("identity", {})
    evolution = manifest.get("evolution", {})
    
    manifest_id = identity.get("name", "unknown")
    manifest_version = evolution.get("manifest_version", "1.0.0")
    
    # Get workflow/engine info
    engine = evolution.get("engine", "retrieval_engine")
    engine_version = evolution.get("engine_version", "v1")
    
    print(f"\n{'=' * 60}")
    print("RETRIEVAL ENGINE - EXECUTION START")
    print(f"{'=' * 60}")
    print(f"  UTID: {utid}")
    print(f"  Manifest: {manifest_id}")
    print(f"  Version: {manifest_version}")
    print(f"  Engine: {engine} {engine_version}")
    print(f"{'=' * 60}\n")
    
    # Build execution context
    ctx = {
        "utid": utid,
        "manifest_id": manifest_id,
        "manifest_version": manifest_version,
        "manifest": manifest,
        "semantic_utids": [],
        "doc_ids": [],  # Trace Everything: collect all source doc_ids
        "components": []
    }
    
    try:
        # Phase 1: Fetch Dependencies - Fan-In from Semantic Sources
        print("📥 FETCH DEPENDENCIES PHASE")
        intent = manifest.get("intent", {})
        sources = intent.get("sources", {})
        
        # Get primary source (can be dict or string)
        primary_source = sources.get("primary", {})
        if isinstance(primary_source, dict):
            primary_ref = primary_source.get("manifest_ref", "")
        else:
            primary_ref = primary_source
        
        # Get secondary source (optional)
        secondary_source = sources.get("secondary", {})
        if isinstance(secondary_source, dict):
            secondary_ref = secondary_source.get("manifest_ref", "")
        else:
            secondary_ref = secondary_source if secondary_source else ""
        
        print(f"   Primary: {primary_ref}")
        if secondary_ref:
            print(f"   Secondary: {secondary_ref}")
        else:
            print("   Secondary: (none)")
        
        # Fetch primary semantic projection
        primary_sem = _find_latest_semantic_for_manifest(primary_ref)
        ctx["primary_data"] = primary_sem
        ctx["semantic_utids"].append(primary_sem["metadata"].get("utid", "unknown"))
        # Trace Everything: collect doc_id from source
        primary_doc_id = primary_sem["metadata"].get("doc_id")
        if primary_doc_id and primary_doc_id not in ctx["doc_ids"]:
            ctx["doc_ids"].append(primary_doc_id)
        print(f"   ✅ Bound Primary UTID: {primary_sem['metadata'].get('utid', 'N/A')[:30]}...")
        
        # Fetch secondary semantic projection (optional)
        if secondary_ref:
            try:
                secondary_sem = _find_latest_semantic_for_manifest(secondary_ref)
                ctx["secondary_data"] = secondary_sem
                ctx["semantic_utids"].append(secondary_sem["metadata"].get("utid", "unknown"))
                # Trace Everything: collect doc_id from source
                secondary_doc_id = secondary_sem["metadata"].get("doc_id")
                if secondary_doc_id and secondary_doc_id not in ctx["doc_ids"]:
                    ctx["doc_ids"].append(secondary_doc_id)
                print(f"   ✅ Bound Secondary UTID: {secondary_sem['metadata'].get('utid', 'N/A')[:30]}...")
            except ValueError:
                print(f"   ⚠️  Secondary source not found (optional)")
                ctx["secondary_data"] = None
        
        # Phase 2: Synthesis - Join semantic projections
        print("\n⚙️  SYNTHESIS PHASE")
        
        # Get workflow steps to find join component
        workflow = manifest.get("workflow", {})
        steps = workflow.get("steps", [])
        
        # Find the join step
        join_step = None
        for step in steps:
            if step.get("component") or "join" in step.get("name", ""):
                join_step = step
                break
        
        if join_step:
            component_name = join_step.get("component", "temporal_joiner")
            params = join_step.get("params", {})
        else:
            # Fallback defaults
            component_name = "temporal_joiner"
            params = {}
        
        component_version = "1.0.0"
        
        print(f"   [{component_name}] Resolving from v1 library")
        
        # Import and execute the synthesis component
        module_path = f"mda_platform.execution_plane.engines.retrieval_engine.python.v1.{component_name}"
        try:
            component_module = importlib.import_module(module_path)
        except ImportError:
            # Fallback to generic temporal_joiner
            module_path = "mda_platform.execution_plane.engines.retrieval_engine.python.v1.temporal_joiner"
            component_module = importlib.import_module(module_path)
        
        result = component_module.run(ctx, params)
        print(f"   ✅ {result}")
        
        ctx["components"].append({
            "step": "synthesis",
            "path": module_path,
            "version": component_version,
            "status": "SUCCESS"
        })
        
        # Phase 3: Format and Write Output
        print("\n📤 OUTPUT PHASE")
        _RETRIEVAL_STORE.mkdir(parents=True, exist_ok=True)
        
        # Get output format from intent (new) or governance (legacy)
        output_format = intent.get("output_format", "jsonld")
        domain = identity.get("domain", "unknown")
        
        # Generate sequenced filename: retrieval-0001-utid-<utid>.json
        from mda_platform.execution_plane.common.connectors.sequence_counter import next_seq
        seq = next_seq("retrieval_store")
        # Extract UUID portion from utid (remove utid- prefix if present)
        utid_uuid = utid.replace("utid-", "")
        output_filename = f"retrieval-{seq:04d}-utid-{utid_uuid}.{output_format}"
        output_path = _RETRIEVAL_STORE / output_filename
        
        output = {
            "metadata": {
                "utid": utid,
                "semantic_utids": ctx["semantic_utids"],
                "manifest_id": manifest_id,
                "manifest_version": manifest_version,
                "domain": domain,
                "engine": engine,
                "engine_version": engine_version,
                "output_format": output_format,
                "record_count": len(ctx.get("joined_data", [])),
                "created_at": datetime.now(timezone.utc).isoformat()
            },
            "lineage": {
                "primary_source": primary_ref,
                "secondary_source": secondary_ref,
                "join_key": params.get("join_key"),
                "join_type": params.get("join_type")
            },
            "data": ctx.get("joined_data", [])
        }
        
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        
        ctx["retrieval_store_path"] = str(output_path)
        print(f"   ✅ Written: {output_filename}")
        print(f"   Format: {output_format}")
        print(f"   Records: {output['metadata']['record_count']}")
        
        # Phase 4: Evidence
        print(f"\n{'=' * 60}")
        print("✅ EXECUTION COMPLETE")
        print("   BOM recorded in Evidence Store")
        print(f"{'=' * 60}\n")
        
        # Write evidence
        EvidenceStore.write_retrieval(
            utid=utid,
            manifest_id=manifest_id,
            manifest_version=manifest_version,
            semantic_utids=ctx["semantic_utids"],
            source_manifests=[primary_ref, secondary_ref] if secondary_ref else [primary_ref],
            domain=domain,
            engine=engine,
            engine_version=engine_version,
            output_path=str(output_path),
            output_format=output_format,
            record_count=output["metadata"]["record_count"],
            components=ctx["components"],
            doc_ids=ctx["doc_ids"],  # Trace Everything: source documents
            status="SUCCESS"
        )
        
        return {
            "status": "SUCCESS",
            "utid": utid,
            "manifest_id": manifest_id,
            "semantic_utids": ctx["semantic_utids"],
            "output_path": str(output_path),
            "record_count": output["metadata"]["record_count"],
            "components": len(ctx["components"])
        }
        
    except Exception as e:
        print(f"\n{'=' * 60}")
        print(f"❌ EXECUTION FAILED: {e}")
        print("   Error recorded in Evidence Store")
        print(f"{'=' * 60}\n")
        
        EvidenceStore.write_retrieval(
            utid=utid,
            manifest_id=manifest_id,
            manifest_version=manifest_version,
            semantic_utids=ctx.get("semantic_utids", []),
            source_manifests=[],
            domain=manifest.get("metadata", {}).get("domain", "unknown"),
            engine=engine,
            engine_version=engine_version,
            output_path=None,
            output_format="json",
            record_count=0,
            components=ctx.get("components", []),
            doc_ids=ctx.get("doc_ids", []),  # Trace Everything: source documents
            status="FAILURE",
            error=str(e)
        )
        
        return {
            "status": "FAILURE",
            "utid": utid,
            "manifest_id": manifest_id,
            "error": str(e)
        }
