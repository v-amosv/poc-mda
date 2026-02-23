# platform/control_plane/orchestrator/publisher.py
"""
MDA Orchestrator Publisher

The "Birth Chamber" for UTIDs.
Responsible for:
  1. Minting the UTID (governance event)
  2. Recording QUEUED status in Evidence Store
  3. Dispatching to the appropriate Engine

This is where the "intent to execute" is first recorded.
Even if the engine never runs, we have evidence of the attempt.
"""
import uuid
import sys
from pathlib import Path

# Add project root to path (mda-poc-v2)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from mda_platform.execution_plane.common.connectors.evidence_store import EvidenceStore
from mda_platform.control_plane.manifest_store import ManifestStore


def mint_utid() -> str:
    """Generate a new Unified Trace ID."""
    return f"utid-{uuid.uuid4()}"


def trigger_curation_job(manifest_id: str, version: str = None) -> str:
    """
    Trigger execution of a curation manifest.
    
    This is the governance event where UTID is born.
    
    Args:
        manifest_id: The manifest to execute (e.g., 'bls_employment_stats')
        version: Optional specific version (defaults to latest deployed)
        
    Returns:
        The UTID for tracking this execution
        
    Raises:
        ValueError: If manifest is not deployed
    """
    # 1. VALIDATE: Ensure manifest is deployed (specific version or latest)
    deployed = ManifestStore.get_deployed(manifest_id, version)
    if not deployed:
        if version:
            # Check what versions are available
            all_versions = ManifestStore.get_all_versions(manifest_id)
            if all_versions:
                raise ValueError(
                    f"Version {version} not found for '{manifest_id}'. "
                    f"Available versions: {', '.join(all_versions)}"
                )
            else:
                raise ValueError(
                    f"Cannot trigger '{manifest_id}': not deployed. "
                    "Run deploy.py first."
                )
        else:
            raise ValueError(
                f"Cannot trigger '{manifest_id}': not deployed. "
                "Run deploy.py first."
            )
    
    deployed_version = deployed["version"]
    
    # 2. MINT: The UTID is born here
    utid = mint_utid()
    
    # 3. LOG INTENT: Record that we intend to run this manifest
    # This is the first evidence - even if engine fails to start,
    # we have a record of the attempt
    EvidenceStore.write_uir(utid, {
        "status": "QUEUED",
        "layer": "curation",
        "manifest_id": manifest_id,
        "manifest_version": deployed_version,
        "content_hash": deployed["content_hash"]
    })
    
    print(f"🎫 CURATION UTID MINTED: {utid}")
    print(f"   Manifest: {manifest_id} (v{deployed_version})")
    print(f"   Status: QUEUED")
    
    return utid


def trigger_semantic_job(manifest_id: str, version: str = None) -> str:
    """
    Trigger execution of a semantic manifest.
    
    Same governance pattern as curation: UTID is born here.
    
    Args:
        manifest_id: The semantic manifest to execute
        version: Optional specific version (defaults to latest deployed)
        
    Returns:
        The UTID for tracking this execution
        
    Raises:
        ValueError: If manifest is not deployed
    """
    # 1. VALIDATE: Ensure manifest is deployed
    deployed = ManifestStore.get_deployed(manifest_id, version)
    if not deployed:
        raise ValueError(
            f"Cannot trigger semantic '{manifest_id}': not deployed. "
            "Run deploy.py first."
        )
    
    deployed_version = deployed["version"]
    
    # 2. MINT: The UTID is born here
    utid = mint_utid()
    
    # 3. LOG INTENT: Record that we intend to run this manifest
    EvidenceStore.write_uir(utid, {
        "status": "QUEUED",
        "layer": "semantics",
        "manifest_id": manifest_id,
        "manifest_version": deployed_version,
        "content_hash": deployed["content_hash"]
    })
    
    print(f"🎫 SEMANTIC UTID MINTED: {utid}")
    print(f"   Manifest: {manifest_id} (v{deployed_version})")
    print(f"   Status: QUEUED")
    
    return utid

def trigger_replay(manifest_id: str, version: str, source_utid: str) -> str:
    """
    Trigger a replay of a historical execution.
    
    A replay uses the immutable Raw data from the original execution
    instead of re-ingesting from Wild (which may have changed).
    
    Args:
        manifest_id: The manifest to execute
        version: The specific version to run (from original execution)
        source_utid: The UTID of the original execution to replay
        
    Returns:
        The new UTID for tracking this replay execution
        
    Raises:
        ValueError: If manifest version not found
    """
    # 1. VALIDATE: Ensure the manifest version exists
    deployed = ManifestStore.get_deployed(manifest_id, version)
    if not deployed:
        raise ValueError(
            f"Cannot replay: version {version} of '{manifest_id}' not found"
        )
    
    # 2. MINT: A new UTID is born for the replay
    utid = mint_utid()
    
    # 3. LOG INTENT: Record the replay attempt with source reference
    EvidenceStore.write_uir(utid, {
        "status": "QUEUED",
        "manifest_id": manifest_id,
        "manifest_version": version,
        "content_hash": deployed["content_hash"],
        "replay_mode": True,
        "source_utid": source_utid
    })
    
    print(f"🎫 UTID MINTED (REPLAY): {utid}")
    print(f"   Manifest: {manifest_id} (v{version})")
    print(f"   Source: {source_utid}")
    print(f"   Status: QUEUED")
    
    return utid


def dispatch_to_curation_engine(utid: str, manifest_id: str) -> dict:
    """
    Dispatch execution to the Curation Engine.
    
    In production, this would push to a queue.
    For POC, we directly invoke the interpreter.
    
    Args:
        utid: The Unified Trace ID
        manifest_id: The manifest to execute
        
    Returns:
        Execution result from the engine
    """
    print(f"\n📤 DISPATCHING TO CURATION ENGINE...")
    
    # Import here to avoid circular imports
    from mda_platform.execution_plane.engines.curation_engine import interpreter as curation_interpreter
    
    return curation_interpreter.execute(utid, manifest_id)


def dispatch_to_semantic_engine(utid: str, manifest: dict) -> dict:
    """
    Dispatch execution to the Semantic Engine.
    
    Args:
        utid: The Unified Trace ID
        manifest: The parsed manifest dict
        
    Returns:
        Execution result from the engine
    """
    print(f"\n📤 DISPATCHING TO SEMANTIC ENGINE...")
    
    from mda_platform.execution_plane.engines.semantic_engine import interpreter as semantic_interpreter
    
    return semantic_interpreter.execute(manifest, utid)
