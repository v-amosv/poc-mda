# platform/execution_plane/common/connectors/evidence_store.py
"""
Evidence Store Connector

The Evidence Store is the immutable audit log for all MDA operations.

Record Types:
  - Curation Execution Record: Curation evidence
    Filename: curation_0001_<manifest_id>_v<version>.json
    
  - Semantic Execution Record: Semantic evidence
    Filename: semantic_0001_<manifest_id>_v<version>.json
    
  - Retrieval Execution Record: Retrieval evidence
    Filename: retrieval_0001_<manifest_id>_v<version>.json
    
  - DER (Deployment Evidence Record): Deployment evidence
    Filename: deploy_{seq:04d}_{manifest_id}_v{version}.json

For POC: JSON file-based storage in storage_plane/evidence_store/
"""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict

# Resolve path relative to project root (mda-poc-v2)
_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
EVIDENCE_STORE_PATH = _PROJECT_ROOT / "mda_platform" / "storage_plane" / "evidence_store"

# Import sequence counter
import sys
sys.path.insert(0, str(_PROJECT_ROOT))
from mda_platform.execution_plane.common.connectors.sequence_counter import format_filename, get_current_seq


class EvidenceStore:
    """
    Static methods for writing/reading execution evidence records.
    """
    
    # Cache mapping utid -> filename (for updates to existing records)
    _utid_to_filename: Dict[str, str] = {}

    @staticmethod
    def _get_record_path(utid: str, manifest_id: str = None, version: str = None, 
                         record_type: str = "curation", create_new: bool = False) -> Path:
        """Get the file path for an evidence record.
        
        Args:
            utid: The Unified Trace ID
            manifest_id: The manifest identifier
            version: The manifest version
            record_type: One of 'curation', 'semantic', 'retrieval'
            create_new: If True, generate a new sequenced filename
        """
        # Check cache first
        if utid in EvidenceStore._utid_to_filename:
            return EVIDENCE_STORE_PATH / EvidenceStore._utid_to_filename[utid]
        
        # Search for existing file with this utid
        if EVIDENCE_STORE_PATH.exists():
            for f in EVIDENCE_STORE_PATH.glob(f"{record_type}_*_{manifest_id or '*'}*.json"):
                with open(f, "r") as file:
                    try:
                        data = json.load(file)
                        if data.get("utid") == utid:
                            EvidenceStore._utid_to_filename[utid] = f.name
                            return f
                    except:
                        continue
        
        # Create new filename with sequence
        if create_new and manifest_id and version:
            from mda_platform.execution_plane.common.connectors.sequence_counter import next_seq
            seq = next_seq(record_type)
            safe_manifest_id = manifest_id.replace("/", "_")
            filename = f"{record_type}_{seq:04d}_{safe_manifest_id}_v{version}.json"
            EvidenceStore._utid_to_filename[utid] = filename
            return EVIDENCE_STORE_PATH / filename
        
        # Fallback for reads of non-existent records
        return EVIDENCE_STORE_PATH / f"{utid}.json"

    @staticmethod
    def write_uir(utid: str, data: dict, manifest_id: str = None, version: str = None) -> None:
        """
        Write or update an Execution Record (curation, semantic, or retrieval).
        
        Ensures utid and doc_id are the first two fields for traceability.
        
        Args:
            utid: The Unified Trace ID
            data: Record data (status, manifest_id, timestamps, layer, etc.)
            manifest_id: The manifest identifier (for filename)
            version: The manifest version (for filename)
        """
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        
        # Extract manifest_id/version from data if not provided
        manifest_id = manifest_id or data.get("manifest_id")
        version = version or data.get("manifest_version")
        
        # Determine record type from layer (default to curation for backward compatibility)
        layer = data.get("layer", "curation")
        # Normalize "semantics" -> "semantic" for filename consistency
        record_type = "semantic" if layer in ("semantic", "semantics") else layer
        
        # Check if record already exists (update) or new (create with sequence)
        record_path = EvidenceStore._get_record_path(
            utid, manifest_id=manifest_id, version=version, 
            record_type=record_type, create_new=True
        )
        
        # Load existing record if present (for updates)
        existing = {}
        if record_path.exists():
            with open(record_path, "r") as f:
                existing = json.load(f)
        
        # Merge with new data
        existing.update(data)
        existing["utid"] = utid
        existing["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        # Set created_at only on first write
        if "created_at" not in existing:
            existing["created_at"] = existing["updated_at"]
        
        # Ensure utid and doc_id are first two fields (ordered dict)
        doc_id = existing.pop("doc_id", None)
        utid_val = existing.pop("utid")
        ordered = {"utid": utid_val}
        if doc_id:
            ordered["doc_id"] = doc_id
        ordered.update(existing)
        
        with open(record_path, "w") as f:
            json.dump(ordered, f, indent=2)

    @staticmethod
    def read_uir(utid: str) -> Optional[Dict]:
        """
        Read a Unified Invocation Record.
        
        Args:
            utid: The Unified Trace ID
            
        Returns:
            The UIR dict or None if not found
        """
        record_path = EvidenceStore._get_record_path(utid)
        if not record_path.exists():
            return None
        
        with open(record_path, "r") as f:
            return json.load(f)

    @staticmethod
    def update_status(utid: str, status: str, **extra) -> None:
        """
        Convenience method to update status with timestamp.
        
        Args:
            utid: The Unified Trace ID
            status: New status (QUEUED, STARTED, SUCCESS, FAILURE)
            **extra: Additional fields to record
        """
        data = {
            "status": status,
            f"{status.lower()}_at": datetime.now(timezone.utc).isoformat(),
            **extra
        }
        EvidenceStore.write_uir(utid, data)

    @staticmethod
    def write_bom(utid: str, bom: dict) -> None:
        """
        Write the Bill of Materials for a completed run.
        
        Args:
            utid: The Unified Trace ID
            bom: The BOM containing component versions, inputs, outputs
        """
        EvidenceStore.write_uir(utid, {"bom": bom})

    @staticmethod
    def list_all() -> List[Dict]:
        """List all UIR records."""
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        records = []
        for f in EVIDENCE_STORE_PATH.glob("utid-*.json"):
            with open(f, "r") as file:
                records.append(json.load(file))
        return records

    @staticmethod
    def find_first_success(manifest_id: str, version: str) -> Optional[Dict]:
        """
        Find the first successful execution for a manifest+version.
        
        Used for replay-by-version: finds the original execution
        whose Raw data can be replayed.
        
        Args:
            manifest_id: The manifest identifier
            version: The manifest version
            
        Returns:
            The UIR dict of the first successful execution, or None
        """
        records = EvidenceStore.list_all()
        
        # Filter to matching manifest+version with SUCCESS status
        # Exclude replays to find the "original" execution
        matching = [
            r for r in records
            if r.get("manifest_id") == manifest_id
            and r.get("manifest_version") == version
            and r.get("status") == "SUCCESS"
            and not r.get("replay_mode", False)  # Exclude replays
        ]
        
        if not matching:
            return None
        
        # Sort by created_at ascending to get the first (oldest)
        matching.sort(key=lambda x: x.get("created_at", ""))
        
        return matching[0]

    # =========================================================================
    # DEPLOYMENT EVIDENCE RECORDS (DER)
    # =========================================================================
    
    @staticmethod
    def write_deployment(
        manifest_id: str,
        manifest_version: str,
        content_hash: str,
        layer: str,
        agency: str,
        source_path: str,
        target_path: str,
        engine: str = None,
        engine_version: str = None,
        status: str = "SUCCESS",
        **extra
    ) -> str:
        """
        Write a Deployment Evidence Record.
        
        Records when a manifest is deployed from Registry to Manifest Store.
        
        Args:
            manifest_id: The manifest identifier
            manifest_version: The manifest version
            content_hash: Hash of the manifest content
            layer: curation/semantics/retrieval
            agency: The agency (census, bls, etc.)
            source_path: Registry path
            target_path: Manifest store path
            engine: The execution engine (python, python_spark, etc.)
            engine_version: The engine version
            status: SUCCESS or FAILURE
            **extra: Additional fields
            
        Returns:
            deployment_id: Unique deployment identifier
        """
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        
        deployment_id = f"deploy_{uuid.uuid4()}"
        timestamp = datetime.now(timezone.utc).isoformat()
        
        record = {
            "deployment_id": deployment_id,
            "record_type": "deployment",
            "manifest_id": manifest_id,
            "manifest_version": manifest_version,
            "content_hash": content_hash,
            "layer": layer,
            "agency": agency,
            "engine": engine,
            "engine_version": engine_version,
            "source_path": source_path,
            "target_path": target_path,
            "status": status,
            "deployed_at": timestamp,
            "created_at": timestamp,
            **extra
        }
        
        # Filename: deploy_0001_census_population_v1.0.0.json
        # Replace / with _ in manifest_id to create valid filename
        safe_manifest_id = manifest_id.replace("/", "_")
        filename = f"deploy_{EvidenceStore._next_deploy_seq():04d}_{safe_manifest_id}_v{manifest_version}.json"
        filepath = EVIDENCE_STORE_PATH / filename
        
        with open(filepath, "w") as f:
            json.dump(record, f, indent=2)
        
        return deployment_id
    
    @staticmethod
    def _next_deploy_seq() -> int:
        """Get next sequence number for deployment records."""
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        existing = list(EVIDENCE_STORE_PATH.glob("deploy_*.json"))
        return len(existing) + 1
    
    @staticmethod
    def list_deployments(manifest_id: str = None) -> List[Dict]:
        """
        List deployment records.
        
        Args:
            manifest_id: Optional filter by manifest_id
            
        Returns:
            List of deployment evidence records
        """
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        records = []
        
        for f in sorted(EVIDENCE_STORE_PATH.glob("deploy_*.json")):
            with open(f, "r") as file:
                record = json.load(file)
                if manifest_id is None or record.get("manifest_id") == manifest_id:
                    records.append(record)
        
        return records
    
    @staticmethod
    def get_deployment_history(manifest_id: str, version: str = None) -> List[Dict]:
        """
        Get deployment history for a manifest.
        
        Args:
            manifest_id: The manifest identifier
            version: Optional version filter
            
        Returns:
            List of deployment records, newest first
        """
        records = EvidenceStore.list_deployments(manifest_id)
        
        if version:
            records = [r for r in records if r.get("version") == version]
        
        # Sort by deployed_at descending (newest first)
        records.sort(key=lambda x: x.get("deployed_at", ""), reverse=True)
        
        return records

    # =========================================================================
    # SEMANTIC EVIDENCE RECORDS (SER)
    # =========================================================================
    
    @staticmethod
    def write_semantic(
        utid: str,
        manifest_id: str,
        manifest_version: str,
        curation_utid: str,
        source_manifest_ref: str,
        domain: str,
        engine: str,
        engine_version: str,
        output_path: str,
        record_count: int,
        components: List[Dict],
        status: str = "SUCCESS",
        error: str = None,
        doc_id: str = None,
        **extra
    ) -> str:
        """
        Write a Semantic Evidence Record.
        
        Records semantic projection execution, linking back to curation UTID.
        
        Args:
            utid: Semantic UTID
            doc_id: Document identifier (semantic output filename)
            manifest_id: The semantic manifest identifier
            manifest_version: The manifest version
            curation_utid: Upstream curation UTID (lineage link)
            source_manifest_ref: The curation manifest referenced
            domain: Semantic domain (macroeconomics, demographics, etc.)
            engine: The execution engine
            engine_version: The engine version
            output_path: Path to semantic output
            record_count: Number of records projected
            components: List of component execution records
            status: SUCCESS or FAILURE
            error: Error message if failed
            **extra: Additional fields
            
        Returns:
            semantic_evidence_id: Unique identifier
        """
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now(timezone.utc).isoformat()
        
        record = {
            "utid": utid,
            "doc_id": doc_id,
            "record_type": "semantic",
            "manifest_id": manifest_id,
            "manifest_version": manifest_version,
            "curation_utid": curation_utid,
            "source_manifest_ref": source_manifest_ref,
            "domain": domain,
            "engine": engine,
            "engine_version": engine_version,
            "output_path": output_path,
            "record_count": record_count,
            "components": components,
            "status": status,
            "error": error,
            "executed_at": timestamp,
            "created_at": timestamp,
            **extra
        }
        
        # Generate sequenced filename: semantic_0001_<manifest_id>_v<version>.json
        from mda_platform.execution_plane.common.connectors.sequence_counter import next_seq
        seq = next_seq("semantic")
        safe_manifest_id = manifest_id.replace("/", "_")
        filename = f"semantic_{seq:04d}_{safe_manifest_id}_v{manifest_version}.json"
        filepath = EVIDENCE_STORE_PATH / filename
        
        with open(filepath, "w") as f:
            json.dump(record, f, indent=2)
        
        return utid

    # =========================================================================
    # RETRIEVAL EVIDENCE RECORDS (RER)
    # =========================================================================
    
    @staticmethod
    def write_retrieval(
        utid: str,
        manifest_id: str,
        manifest_version: str,
        semantic_utids: List[str],
        source_manifests: List[str],
        domain: str,
        engine: str,
        engine_version: str,
        output_path: str,
        output_format: str,
        record_count: int,
        components: List[Dict],
        status: str = "SUCCESS",
        error: str = None,
        doc_ids: List[str] = None,
        **extra
    ) -> str:
        """
        Write a Retrieval Evidence Record.
        
        Records retrieval execution, linking back to multiple semantic UTIDs.
        This is the "Grandmother-Parent-Child" link in the UTID chain.
        
        Trace Everything Principle: Each answer must be traceable back to 
        the source document(s) via doc_ids.
        
        Args:
            utid: Retrieval UTID
            manifest_id: The retrieval manifest identifier
            manifest_version: The manifest version
            semantic_utids: List of upstream semantic UTIDs (lineage links)
            source_manifests: List of semantic manifests joined
            domain: Output domain
            engine: The execution engine
            engine_version: The engine version
            output_path: Path to retrieval output
            output_format: Output format (json-ld, csv, etc.)
            record_count: Number of records in output
            components: List of component execution records
            status: SUCCESS or FAILURE
            error: Error message if failed
            doc_ids: List of source document IDs (Trace Everything)
            **extra: Additional fields
            
        Returns:
            retrieval_evidence_id: Unique identifier
        """
        EVIDENCE_STORE_PATH.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now(timezone.utc).isoformat()
        
        record = {
            "utid": utid,
            "doc_ids": doc_ids or [],  # Trace Everything: source documents
            "record_type": "retrieval",
            "manifest_id": manifest_id,
            "manifest_version": manifest_version,
            "semantic_utids": semantic_utids,
            "source_manifests": source_manifests,
            "domain": domain,
            "engine": engine,
            "engine_version": engine_version,
            "output_path": output_path,
            "output_format": output_format,
            "record_count": record_count,
            "components": components,
            "status": status,
            "error": error,
            "executed_at": timestamp,
            "created_at": timestamp,
            **extra
        }
        
        # Generate sequenced filename: retrieval_0001_<manifest_id>_v<version>.json
        from mda_platform.execution_plane.common.connectors.sequence_counter import next_seq
        seq = next_seq("retrieval")
        safe_manifest_id = manifest_id.replace("/", "_")
        filename = f"retrieval_{seq:04d}_{safe_manifest_id}_v{manifest_version}.json"
        filepath = EVIDENCE_STORE_PATH / filename
        
        with open(filepath, "w") as f:
            json.dump(record, f, indent=2)
        
        return utid
