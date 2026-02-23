#!/usr/bin/env python3
# trace.py
"""
MDA Lineage Trace Tool

Traces the full lineage chain from any UTID back to source document(s).
Implements the "Trace Everything Principle" - every answer must be 
traceable back to the source document(s).

Usage:
    uv run trace.py <utid>
    uv run trace.py --latest              # Trace most recent execution
    uv run trace.py --latest curation     # Most recent curation
    uv run trace.py --latest semantic     # Most recent semantic
    uv run trace.py --latest retrieval    # Most recent retrieval

Examples:
    uv run trace.py utid-abc123-def456
    uv run trace.py --latest retrieval
"""
import argparse
import json
import sys
from pathlib import Path
from typing import Optional, Dict, List

PROJECT_ROOT = Path(__file__).parent
STORAGE_PLANE = PROJECT_ROOT / "mda_platform" / "storage_plane"
EVIDENCE_STORE = STORAGE_PLANE / "evidence_store"
FACT_STORE = STORAGE_PLANE / "fact_store"
SEMANTIC_STORE = STORAGE_PLANE / "semantic_store"
RAW_STORE = STORAGE_PLANE / "raw_store"


def find_evidence_by_utid(utid: str) -> Optional[Dict]:
    """Find evidence record by UTID."""
    if not EVIDENCE_STORE.exists():
        return None
    
    for evidence_file in EVIDENCE_STORE.glob("*.json"):
        with open(evidence_file) as f:
            record = json.load(f)
        if record.get("utid") == utid:
            return record
    return None


def find_latest_evidence(record_type: str = None) -> Optional[Dict]:
    """Find the most recent evidence record, optionally filtered by type."""
    if not EVIDENCE_STORE.exists():
        return None
    
    latest = None
    latest_time = None
    
    for evidence_file in EVIDENCE_STORE.glob("*.json"):
        with open(evidence_file) as f:
            record = json.load(f)
        
        # Skip deploy records (no utid)
        if not record.get("utid"):
            continue
        
        # Filter by layer or record_type (different evidence types use different fields)
        if record_type:
            layer = record.get("layer") or record.get("record_type")
            # Handle semantic/semantics variations
            if record_type in ("semantic", "semantics"):
                if layer not in ("semantic", "semantics"):
                    continue
            elif layer != record_type:
                continue
        
        created_at = record.get("created_at", record.get("executed_at", ""))
        if latest_time is None or created_at > latest_time:
            latest = record
            latest_time = created_at
    
    return latest


def find_fact_by_utid(utid: str) -> Optional[Dict]:
    """Find fact record by UTID."""
    if not FACT_STORE.exists():
        return None
    
    for agency_dir in FACT_STORE.iterdir():
        if not agency_dir.is_dir():
            continue
        for fact_file in agency_dir.glob("fact-*.json"):
            if utid.replace("utid-", "") in fact_file.name:
                with open(fact_file) as f:
                    return json.load(f)
    return None


def find_semantic_by_utid(utid: str) -> Optional[Dict]:
    """Find semantic record by UTID."""
    if not SEMANTIC_STORE.exists():
        return None
    
    for domain_dir in SEMANTIC_STORE.iterdir():
        if not domain_dir.is_dir():
            continue
        for sem_file in domain_dir.glob("semantic-*.json"):
            if utid.replace("utid-", "") in sem_file.name:
                with open(sem_file) as f:
                    return json.load(f)
    return None


def find_raw_by_utid(utid: str) -> Optional[Path]:
    """Find raw file path by UTID."""
    if not RAW_STORE.exists():
        return None
    
    for agency_dir in RAW_STORE.iterdir():
        if not agency_dir.is_dir():
            continue
        for raw_file in agency_dir.glob("raw-*.json"):
            if utid.replace("utid-", "") in raw_file.name:
                return raw_file
    return None


def extract_source_file(evidence: Dict) -> str:
    """Extract source file name from evidence (BOM or execution log)."""
    bom = evidence.get("bom", {})
    
    # First try direct BOM fields (new format)
    wild_source = bom.get("wild_source")
    if wild_source:
        return wild_source
    
    # Fallback: parse from execution_log (legacy format)
    execution_log = bom.get("execution_log", [])
    for entry in execution_log:
        if entry.get("step") == "ingestion":
            result = entry.get("result", "")
            # Parse: "INGEST_SUCCESS: employment_stats.csv -> raw-..."
            if ":" in result and "->" in result:
                parts = result.split(":")
                if len(parts) > 1:
                    file_part = parts[1].strip().split("->")[0].strip()
                    return file_part
    return "unknown"


def extract_raw_doc(evidence: Dict) -> str:
    """Extract raw document filename from evidence (BOM or execution log)."""
    bom = evidence.get("bom", {})
    
    # First try direct BOM field (new format)
    raw_doc = bom.get("raw_doc")
    if raw_doc:
        return raw_doc
    
    # Fallback: parse from execution_log (legacy format)
    execution_log = bom.get("execution_log", [])
    for entry in execution_log:
        if entry.get("step") == "ingestion":
            result = entry.get("result", "")
            # Parse: "INGEST_SUCCESS: employment_stats.csv -> raw-..."
            if "->" in result:
                parts = result.split("->")
                if len(parts) > 1:
                    return parts[1].strip()
    return "unknown"


def trace_curation(evidence: Dict, indent: int = 0) -> Dict:
    """Trace a curation execution."""
    prefix = "  " * indent
    utid = evidence.get("utid", "unknown")
    doc_id = evidence.get("doc_id", "unknown")
    manifest_id = evidence.get("manifest_id", "unknown")
    manifest_version = evidence.get("manifest_version", "unknown")
    status = evidence.get("status", "unknown")
    
    print(f"{prefix}├── 📦 CURATION: {manifest_id} v{manifest_version}")
    print(f"{prefix}│     UTID: {utid}")
    print(f"{prefix}│     doc_id: {doc_id}")
    print(f"{prefix}│     Status: {'✅' if status == 'SUCCESS' else '❌'} {status}")
    
    # Extract source and raw doc from evidence
    source_file = extract_source_file(evidence)
    raw_doc = extract_raw_doc(evidence)
    print(f"{prefix}│     Wild Source: wild/{source_file}")
    print(f"{prefix}│     Raw Doc: raw/{raw_doc}")
    
    return {
        "type": "curation",
        "utid": utid,
        "doc_id": doc_id,
        "manifest_id": manifest_id,
        "wild_source": source_file,
        "raw_doc": raw_doc,
        "status": status
    }


def trace_semantic(evidence: Dict, indent: int = 0) -> Dict:
    """Trace a semantic execution and its curation parent."""
    prefix = "  " * indent
    utid = evidence.get("utid", "unknown")
    doc_id = evidence.get("doc_id", "unknown")
    manifest_id = evidence.get("manifest_id", "unknown")
    manifest_version = evidence.get("manifest_version", "unknown")
    curation_utid = evidence.get("curation_utid", "unknown")
    status = evidence.get("status", "unknown")
    
    print(f"{prefix}├── 🧠 SEMANTIC: {manifest_id} v{manifest_version}")
    print(f"{prefix}│     UTID: {utid}")
    print(f"{prefix}│     doc_id: {doc_id}")
    print(f"{prefix}│     Status: {'✅' if status == 'SUCCESS' else '❌'} {status}")
    print(f"{prefix}│     ↓ curation_utid: {curation_utid}")
    
    # Trace parent curation
    curation_evidence = find_evidence_by_utid(curation_utid)
    curation_info = None
    if curation_evidence:
        curation_info = trace_curation(curation_evidence, indent + 1)
    else:
        print(f"{prefix}│     └── ⚠️  Curation evidence not found")
    
    return {
        "type": "semantic",
        "utid": utid,
        "doc_id": doc_id,
        "manifest_id": manifest_id,
        "curation_utid": curation_utid,
        "curation": curation_info,
        "status": status
    }


def trace_retrieval(evidence: Dict, indent: int = 0) -> Dict:
    """Trace a retrieval execution and all its semantic parents."""
    prefix = "  " * indent
    utid = evidence.get("utid", "unknown")
    doc_ids = evidence.get("doc_ids", [])
    manifest_id = evidence.get("manifest_id", "unknown")
    manifest_version = evidence.get("manifest_version", "unknown")
    semantic_utids = evidence.get("semantic_utids", [])
    status = evidence.get("status", "unknown")
    record_count = evidence.get("record_count", 0)
    
    print(f"{prefix}├── 🔍 RETRIEVAL: {manifest_id} v{manifest_version}")
    print(f"{prefix}│     UTID: {utid}")
    print(f"{prefix}│     doc_ids: {doc_ids}")
    print(f"{prefix}│     Records: {record_count}")
    print(f"{prefix}│     Status: {'✅' if status == 'SUCCESS' else '❌'} {status}")
    print(f"{prefix}│     ↓ semantic_utids: {semantic_utids}")
    
    # Trace parent semantics
    semantic_infos = []
    for sem_utid in semantic_utids:
        sem_evidence = find_evidence_by_utid(sem_utid)
        if sem_evidence:
            sem_info = trace_semantic(sem_evidence, indent + 1)
            semantic_infos.append(sem_info)
        else:
            print(f"{prefix}│     └── ⚠️  Semantic evidence not found: {sem_utid}")
    
    return {
        "type": "retrieval",
        "utid": utid,
        "doc_ids": doc_ids,
        "manifest_id": manifest_id,
        "semantic_utids": semantic_utids,
        "semantics": semantic_infos,
        "status": status
    }


def trace(utid: str) -> Dict:
    """
    Trace full lineage for a UTID.
    
    Returns a nested dict representing the full lineage.
    """
    print(f"\n{'=' * 60}")
    print("MDA LINEAGE TRACE - Trace Everything Principle")
    print(f"{'=' * 60}\n")
    
    evidence = find_evidence_by_utid(utid)
    
    if not evidence:
        print(f"❌ No evidence found for UTID: {utid}")
        return {"error": "not_found", "utid": utid}
    
    # Check both layer and record_type fields
    layer = evidence.get("layer") or evidence.get("record_type", "unknown")
    print(f"🔗 LINEAGE CHAIN for: {utid}")
    print(f"   Layer: {layer.upper()}")
    print()
    
    if layer == "retrieval":
        lineage = trace_retrieval(evidence)
    elif layer in ("semantic", "semantics"):
        lineage = trace_semantic(evidence)
    elif layer == "curation":
        lineage = trace_curation(evidence)
    else:
        # Try BOM records (curation without layer field)
        if "components_used" in evidence or evidence.get("bom", {}).get("components_used"):
            lineage = trace_curation(evidence)
        else:
            print(f"⚠️  Unknown layer: {layer}")
            lineage = {"type": "unknown", "evidence": evidence}
    
    # Verify lineage
    print()
    print(f"{'=' * 60}")
    
    # Collect all doc_ids in the chain
    all_doc_ids = set()
    if lineage.get("doc_id"):
        all_doc_ids.add(lineage["doc_id"])
    if lineage.get("doc_ids"):
        all_doc_ids.update(lineage["doc_ids"])
    
    # Recursively collect from children
    def collect_doc_ids(node):
        if isinstance(node, dict):
            if node.get("doc_id"):
                all_doc_ids.add(node["doc_id"])
            if node.get("doc_ids"):
                all_doc_ids.update(node["doc_ids"])
            for v in node.values():
                collect_doc_ids(v)
        elif isinstance(node, list):
            for item in node:
                collect_doc_ids(item)
    
    collect_doc_ids(lineage)
    
    if all_doc_ids:
        print(f"✅ TRACE VERIFIED - Source document(s): {sorted(all_doc_ids)}")
    else:
        print(f"⚠️  No doc_id found in lineage chain")
    
    print(f"{'=' * 60}\n")
    
    return lineage


def main():
    parser = argparse.ArgumentParser(
        description="MDA Lineage Trace - Trace Everything Principle"
    )
    parser.add_argument("utid", nargs="?", help="UTID to trace")
    parser.add_argument("--latest", nargs="?", const="any", 
                       help="Trace latest execution (optionally: curation|semantic|retrieval)")
    
    args = parser.parse_args()
    
    if args.latest:
        record_type = None if args.latest == "any" else args.latest
        evidence = find_latest_evidence(record_type)
        if evidence:
            trace(evidence["utid"])
        else:
            print(f"❌ No {'evidence' if not record_type else record_type + ' evidence'} found")
            sys.exit(1)
    elif args.utid:
        trace(args.utid)
    else:
        parser.print_help()
        print("\nExamples:")
        print("  uv run trace.py utid-abc123-def456")
        print("  uv run trace.py --latest")
        print("  uv run trace.py --latest retrieval")
        sys.exit(1)


if __name__ == "__main__":
    main()
