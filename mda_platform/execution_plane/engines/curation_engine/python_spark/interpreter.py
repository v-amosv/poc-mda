# platform/execution_plane/engines/curation_engine/python_spark/interpreter.py
"""
PySpark Engine Interpreter for Curation Layer

The stateless execution engine for curation manifests using PySpark.
Receives (utid, manifest_id) from the Orchestrator and:
  1. Updates Evidence Store to STARTED
  2. Resolves and executes manifest components via PySpark engine
  3. Writes BOM and final status to Evidence Store

Key MDA Principle: UTID comes FROM the Orchestrator, not created here.
This ensures full traceability from intent to execution.

Engine: python_spark
Components: platform/execution_plane/engines/curation_engine/python_spark/v1/*

Note: For POC, this uses standard Python but simulates Spark-like behavior.
      In production, this would use actual PySpark DataFrames.
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path for imports (mda-poc-v2)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from mda_platform.execution_plane.common.resolver.runtime_resolver import RuntimeResolver
from mda_platform.execution_plane.common.connectors.evidence_store import EvidenceStore
from mda_platform.control_plane.manifest_store import ManifestStore
from mda_platform.control_plane.registry.parser_registry import get_parser


# Engine identity
ENGINE_TYPE = "python_spark"
LAYER = "curation"


class CurationInterpreter:
    """
    Interprets and executes curation manifests using the PySpark engine.
    
    The interpreter is stateless - all state lives in:
      - Manifest Store (what to run)
      - Evidence Store (what happened)
    """
    
    def __init__(self, utid: str, manifest_id: str, version: str = None, source_utid: str = None):
        """
        Initialize the interpreter with execution context.
        
        Args:
            utid: Unified Trace ID (from Orchestrator)
            manifest_id: The manifest to execute
            version: Optional specific version (defaults to latest)
            source_utid: If provided, replay mode uses Raw data from this execution
        """
        self.utid = utid
        self.manifest_id = manifest_id
        self.version = version
        self.source_utid = source_utid
        self.replay_mode = source_utid is not None
        
        # 1. HYDRATE: Load the 'Work Order' from Manifest Store
        self.manifest = ManifestStore.get_manifest_for_execution(manifest_id, version)
        
        # 2. PARSER: Use Control Plane parser for schema-agnostic access
        self.parser = get_parser(LAYER, self.manifest)
        evolution = self.parser.get_evolution()
        
        self.manifest_version = evolution.manifest_version
        self.engine = evolution.engine
        self.engine_version = evolution.engine_version
        
        # Validate engine type matches this interpreter
        if self.engine != ENGINE_TYPE:
            raise ValueError(
                f"Manifest declares engine '{self.engine}', but this is the '{ENGINE_TYPE}' interpreter. "
                f"Use the correct interpreter for engine '{self.engine}'."
            )
        
        # 3. BUILD CONTEXT: Create execution context for components
        self.ctx = {
            "utid": self.utid,
            "manifest_id": manifest_id,
            "manifest_version": self.manifest_version,
            "engine": self.engine,
            "engine_version": self.engine_version,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "manifest": self.manifest,
            "replay_mode": self.replay_mode,
            "source_utid": source_utid
        }
        
        # 4. EVIDENCE: Update status to STARTED
        EvidenceStore.update_status(
            self.utid,
            "STARTED",
            engine=self.engine,
            engine_version=self.engine_version,
            manifest_version=self.manifest_version
        )

    def run(self) -> dict:
        """
        Execute the manifest pipeline.
        
        Returns:
            Execution result summary
        """
        mode_label = "REPLAY" if self.replay_mode else "EXECUTION"
        print(f"\n{'=' * 60}")
        print(f"CURATION ENGINE ({self.engine}) - {mode_label} START")
        print(f"{'=' * 60}")
        print(f"  UTID: {self.utid}")
        print(f"  Manifest: {self.manifest_id}")
        print(f"  Version: {self.ctx['manifest_version']}")
        print(f"  Engine: {self.engine} v{self.engine_version}")
        if self.replay_mode:
            print(f"  ⏮️  REPLAY MODE: Using Raw from {self.source_utid[:20]}...")
        print(f"{'=' * 60}\n")
        
        # Track all component executions for BOM
        bom = {
            "utid": self.utid,
            "manifest_id": self.manifest_id,
            "manifest_version": self.manifest_version,
            "engine": self.engine,
            "engine_version": self.engine_version,
            "components_used": [],
            "execution_log": [],
            "started_at": self.ctx["started_at"]
        }
        
        if self.replay_mode:
            bom["replay_mode"] = True
            bom["source_utid"] = self.source_utid
        
        try:
            # 4. INGESTION PHASE
            if self.replay_mode:
                print("📥 INGESTION PHASE (SKIPPED - REPLAY MODE)")
                print(f"   ⏮️  Using Raw data from: {self.source_utid}")
                self.ctx["raw_source_utid"] = self.source_utid
                ingest_result = f"REPLAY: Using Raw from {self.source_utid}"
                bom["components_used"].append({
                    "step": "ingestion",
                    "path": "REPLAY_MODE",
                    "version": "N/A",
                    "source_utid": self.source_utid
                })
                bom["execution_log"].append({
                    "step": "ingestion",
                    "status": "SKIPPED_REPLAY",
                    "result": ingest_result
                })
            else:
                print("📥 INGESTION PHASE")
                ingest_spec = self.parser.get_ingestion_component()
                print(f"   Resolving: {ingest_spec.path} (v{ingest_spec.version})")
                ingest_fn = RuntimeResolver.resolve_and_validate(
                    {"path": ingest_spec.path, "version": ingest_spec.version},
                    engine=self.engine
                )
                ingest_result = ingest_fn(self.ctx, ingest_spec.params)
                print(f"   ✅ {ingest_result}")
                bom["components_used"].append({
                    "step": "ingestion",
                    "path": ingest_spec.path,
                    "version": ingest_spec.version
                })
                bom["execution_log"].append({
                    "step": "ingestion",
                    "status": "SUCCESS",
                    "result": ingest_result
                })
                
                # Capture lineage: wild source -> raw doc
                if "->" in ingest_result:
                    parts = ingest_result.split(":")[-1].strip().split("->")
                    bom["wild_source"] = parts[0].strip()
                    bom["raw_doc"] = parts[1].strip() if len(parts) > 1 else None

            # 5. PROCESSING PHASE
            print("\n⚙️  PROCESSING PHASE")
            for step in self.parser.get_processing_steps():
                step_name = step.step_name
                step_component = step.component
                print(f"   [{step_name}] Resolving: {step_component.path} (v{step_component.version})")
                process_fn = RuntimeResolver.resolve_and_validate(
                    {"path": step_component.path, "version": step_component.version},
                    engine=self.engine
                )
                process_result = process_fn(self.ctx, step_component.params)
                print(f"   ✅ {process_result}")
                bom["components_used"].append({
                    "step": step_name,
                    "path": step_component.path,
                    "version": step_component.version
                })
                bom["execution_log"].append({
                    "step": step_name,
                    "status": "SUCCESS",
                    "result": process_result
                })

            # 6. SUCCESS
            bom["doc_id"] = self.ctx.get("doc_id", "unknown")
            bom["completed_at"] = datetime.now(timezone.utc).isoformat()
            bom["status"] = "SUCCESS"
            
            EvidenceStore.write_bom(self.utid, bom)
            EvidenceStore.update_status(self.utid, "SUCCESS", doc_id=bom["doc_id"])
            
            print(f"\n{'=' * 60}")
            print(f"✅ EXECUTION COMPLETE")
            print(f"   BOM recorded in Evidence Store")
            print(f"{'=' * 60}\n")
            
            return {"status": "SUCCESS", "utid": self.utid, "bom": bom}

        except Exception as e:
            bom["doc_id"] = self.ctx.get("doc_id", "unknown")
            bom["completed_at"] = datetime.now(timezone.utc).isoformat()
            bom["status"] = "FAILURE"
            bom["error"] = str(e)
            
            EvidenceStore.write_bom(self.utid, bom)
            EvidenceStore.update_status(self.utid, "FAILURE", error=str(e), doc_id=bom["doc_id"])
            
            print(f"\n{'=' * 60}")
            print(f"❌ EXECUTION FAILED: {e}")
            print(f"   Error recorded in Evidence Store")
            print(f"{'=' * 60}\n")

            return {"status": "FAILURE", "utid": self.utid, "error": str(e)}


def execute(utid: str, manifest_id: str, version: str = None, source_utid: str = None) -> dict:
    """Entry point for engine execution."""
    interpreter = CurationInterpreter(utid, manifest_id, version, source_utid)
    return interpreter.run()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python interpreter.py <utid> <manifest_id>")
        sys.exit(1)
    
    result = execute(sys.argv[1], sys.argv[2])
    sys.exit(0 if result["status"] == "SUCCESS" else 1)
