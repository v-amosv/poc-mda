#!/usr/bin/env python3
# mda_platform/execution_plane/engines/semantic_engine/interpreter.py
"""
Semantic Engine Interpreter

The Semantic Engine is the "Ontology Mapper." It takes Curation Facts
and projects them into a Semantic Domain using declarative mapping rules.

Workflow: Hydrate → Source Bind → Map → Enrich → Evidence

Key Concepts:
- Stateless: No hardcoded field names
- Schema-agnostic: All logic from manifest
- UTID Chaining: Semantic UTID links back to Curation UTID
"""
import importlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List

# Resolve project root
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from mda_platform.control_plane.registry.parser_registry import get_parser
from mda_platform.execution_plane.common.connectors.evidence_store import EvidenceStore

# Engine identity
LAYER = "semantics"

# Storage paths
_STORAGE_PLANE = _PROJECT_ROOT / "mda_platform" / "storage_plane"
_FACT_STORE = _STORAGE_PLANE / "fact_store"
_SEMANTIC_STORE = _STORAGE_PLANE / "semantic_store"


def _find_latest_fact_for_manifest(source_manifest_ref: str) -> dict:
    """
    Find the latest fact record for a given curation manifest reference.
    
    Args:
        source_manifest_ref: e.g., "bls_employment_stats"
        
    Returns:
        Dict with fact data and metadata
    """
    # Search all agency folders in fact_store
    for agency_dir in _FACT_STORE.iterdir():
        if not agency_dir.is_dir():
            continue
        for fact_file in sorted(agency_dir.glob("fact-*.json"), reverse=True):
            with open(fact_file) as f:
                fact = json.load(f)
            if source_manifest_ref in fact.get("metadata", {}).get("manifest_id", ""):
                return {
                    "path": str(fact_file),
                    "metadata": fact["metadata"],
                    "data": fact["data"]
                }
    
    raise ValueError(f"No fact found for source manifest: {source_manifest_ref}")


def execute(manifest: dict, utid: str, curation_utid: str = None) -> dict:
    """
    Execute Semantic Engine workflow.
    
    Args:
        manifest: The semantic manifest dict
        utid: Semantic UTID (minted by orchestrator)
        curation_utid: Optional specific curation UTID to bind to
        
    Returns:
        Execution result dict
    """
    parser = get_parser(LAYER, manifest)
    identity = parser.get_identity()
    evolution = parser.get_evolution()
    
    manifest_id = identity.name
    manifest_version = evolution.manifest_version
    engine = evolution.engine
    engine_version = evolution.engine_version
    
    print(f"\n{'=' * 60}")
    print("SEMANTIC ENGINE - EXECUTION START")
    print(f"{'=' * 60}")
    print(f"  UTID: {utid}")
    print(f"  Manifest: {manifest_id}")
    print(f"  Version: {manifest_version}")
    print(f"  Engine: {engine} v{engine_version}")
    print(f"{'=' * 60}\n")
    
    # Build execution context
    ctx = {
        "utid": utid,
        "manifest_id": manifest_id,
        "manifest_version": manifest_version,
        "manifest": manifest,
        "curation_utid": curation_utid,
        "components": []
    }
    
    try:
        # Phase 1: Source Bind - Find the upstream Curation Fact
        print("📥 SOURCE BIND PHASE")
        intent = manifest.get("intent", {})
        source = intent.get("source", {})
        source_manifest_ref = source.get("manifest_ref", "")
        
        print(f"   Source Reference: {source_manifest_ref}")
        
        fact_record = _find_latest_fact_for_manifest(source_manifest_ref)
        ctx["source_fact"] = fact_record
        ctx["curation_utid"] = fact_record["metadata"].get("utid", curation_utid)
        ctx["doc_id"] = fact_record["metadata"].get("doc_id", f"doc-{uuid.uuid4()}")  # Inherit doc_id from curation
        print(f"   ✅ Bound to Curation UTID: {ctx['curation_utid']}")
        print(f"   Source: {Path(fact_record['path']).name}")
        
        # Phase 2: Map - Project physical fields to semantic concepts
        print("\n⚙️  PROJECTION PHASE")
        projection = intent.get("projection", {})
        component_spec = projection.get("component", {})
        mapping = projection.get("mapping", [])
        
        component_path = component_spec.get("path", "")
        component_version = component_spec.get("version", "1.0.0")
        
        # Resolve component from engine library
        print(f"   [{component_path.split('.')[-1]}] Resolving: {component_path} (v{component_version})")
        
        # Import and execute the mapping component
        module_path = f"mda_platform.execution_plane.engines.semantic_engine.python.v1.{component_path.split('.')[-1]}"
        try:
            component_module = importlib.import_module(module_path)
        except ImportError:
            # Fallback to generic ontology_mapper
            module_path = "mda_platform.execution_plane.engines.semantic_engine.python.v1.ontology_mapper"
            component_module = importlib.import_module(module_path)
        
        params = {
            "mapping": mapping,
            "context": intent.get("context", {})
        }
        
        result = component_module.run(ctx, params)
        print(f"   ✅ {result}")
        
        ctx["components"].append({
            "step": "projection",
            "path": module_path,
            "version": component_version,
            "status": "SUCCESS"
        })
        
        # Phase 3: Write Semantic Projection to Semantic Store
        print("\n📤 OUTPUT PHASE")
        _SEMANTIC_STORE.mkdir(parents=True, exist_ok=True)
        
        # Get domain from identity
        domain = manifest.get("identity", {}).get("domain", "unknown")
        domain_dir = _SEMANTIC_STORE / domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate sequenced filename: semantic-0001-utid-<utid>.json
        from mda_platform.execution_plane.common.connectors.sequence_counter import next_seq
        seq = next_seq("semantic_store")
        # Extract UUID portion from utid (remove utid- prefix if present)
        utid_uuid = utid.replace("utid-", "")
        output_filename = f"semantic-{seq:04d}-utid-{utid_uuid}.json"
        output_path = domain_dir / output_filename
        
        output = {
            "metadata": {
                "utid": utid,
                "doc_id": ctx["doc_id"],  # Inherited from curation for traceability
                "curation_utid": ctx["curation_utid"],
                "manifest_id": manifest_id,
                "manifest_version": manifest_version,
                "domain": domain,
                "engine": engine,
                "engine_version": engine_version,
                "source_manifest_ref": source_manifest_ref,
                "record_count": len(ctx.get("projected_data", [])),
                "created_at": datetime.now(timezone.utc).isoformat()
            },
            "context": intent.get("context", {}),
            "data": ctx.get("projected_data", [])
        }
        
        with open(output_path, "w") as f:
            json.dump(output, f, indent=2)
        
        ctx["semantic_store_path"] = str(output_path)
        print(f"   ✅ Written: {output_filename}")
        print(f"   Records: {output['metadata']['record_count']}")
        
        # Phase 4: Evidence
        print(f"\n{'=' * 60}")
        print("✅ EXECUTION COMPLETE")
        print("   BOM recorded in Evidence Store")
        print(f"{'=' * 60}\n")
        
        # Write evidence
        EvidenceStore.write_semantic(
            utid=utid,
            doc_id=ctx["doc_id"],
            manifest_id=manifest_id,
            manifest_version=manifest_version,
            curation_utid=ctx["curation_utid"],
            source_manifest_ref=source_manifest_ref,
            domain=domain,
            engine=engine,
            engine_version=engine_version,
            output_path=str(output_path),
            record_count=output["metadata"]["record_count"],
            components=ctx["components"],
            status="SUCCESS"
        )
        
        return {
            "status": "SUCCESS",
            "utid": utid,
            "manifest_id": manifest_id,
            "curation_utid": ctx["curation_utid"],
            "output_path": str(output_path),
            "record_count": output["metadata"]["record_count"],
            "components": len(ctx["components"])
        }
        
    except Exception as e:
        print(f"\n{'=' * 60}")
        print(f"❌ EXECUTION FAILED: {e}")
        print("   Error recorded in Evidence Store")
        print(f"{'=' * 60}\n")
        
        EvidenceStore.write_semantic(
            utid=utid,
            doc_id=ctx.get("doc_id", "unknown"),
            manifest_id=manifest_id,
            manifest_version=manifest_version,
            curation_utid=ctx.get("curation_utid"),
            source_manifest_ref=source_manifest_ref if 'source_manifest_ref' in dir() else None,
            domain=manifest.get("identity", {}).get("domain", "unknown"),
            engine=engine,
            engine_version=engine_version,
            output_path=None,
            record_count=0,
            components=ctx.get("components", []),
            status="FAILURE",
            error=str(e)
        )
        
        return {
            "status": "FAILURE",
            "utid": utid,
            "manifest_id": manifest_id,
            "error": str(e)
        }
