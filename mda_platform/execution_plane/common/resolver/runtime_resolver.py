# platform/execution_plane/common/resolver/runtime_resolver.py
"""
MDA Runtime Resolver

Translates manifest component references into executable functions.
Engine-aware: resolves paths relative to the engine type.

Path Resolution:
  - Engine-relative paths (e.g., "v1.csv_parser.run") are expanded based on engine type
  - Fully qualified paths are supported for backward compatibility

Examples:
  engine="python", path="v1.csv_parser.run"
  -> platform.execution_plane.engines.curation_engine.python.v1.csv_parser.run

  engine="python_spark", path="v1.csv_parser.run"
  -> platform.execution_plane.engines.curation_engine.python_spark.v1.csv_parser.run
"""
import importlib
import logging
import sys
from pathlib import Path
from typing import Callable, Dict, Any

# Ensure project root is in path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class RuntimeResolver:
    """
    The MDA Runtime Resolver: Translates Manifest ComponentRefs into 
    executable 'Museum of Code' tools.
    
    Engine-aware resolution:
    - Manifest declares engine type (e.g., "python", "python_spark")
    - Component paths are relative to the engine (e.g., "v1.csv_parser.run")
    - Resolver expands to full path based on engine type
    """

    # Supported engine types and their base paths
    ENGINE_BASE_PATHS = {
        "python": "mda_platform.execution_plane.engines.curation_engine.python",
        "python_spark": "mda_platform.execution_plane.engines.curation_engine.python_spark",
        "python_duckdb": "mda_platform.execution_plane.engines.curation_engine.python_duckdb",
    }

    @staticmethod
    def _expand_engine_relative_path(path: str, engine: str) -> str:
        """
        Expand engine-relative path to fully qualified path.
        
        Args:
            path: Component path (e.g., "v1.csv_parser.run")
            engine: Engine type (e.g., "python", "python_spark")
            
        Returns:
            Fully qualified path (e.g., "mda_platform.execution_plane.engines.curation_engine.python.v1.csv_parser.run")
        """
        if engine not in RuntimeResolver.ENGINE_BASE_PATHS:
            raise ValueError(f"Unknown engine type: {engine}. Supported: {list(RuntimeResolver.ENGINE_BASE_PATHS.keys())}")
        
        base = RuntimeResolver.ENGINE_BASE_PATHS[engine]
        return f"{base}.{path}"

    @staticmethod
    def _is_engine_relative_path(path: str) -> bool:
        """
        Check if path is engine-relative (starts with version, e.g., "v1.").
        
        Engine-relative paths start with "v" followed by a digit (v1., v2., etc.)
        """
        return path and path[0] == 'v' and len(path) > 1 and path[1].isdigit()

    @staticmethod
    def _remap_legacy_path(path: str) -> str:
        """
        Remap legacy lib.* paths for backward compatibility.
        lib.engines.curation.v1.x -> platform.execution_plane.engines.curation_engine.python.v1.x
        """
        if path.startswith("lib.engines.curation."):
            # Legacy: lib.engines.curation.v1.x -> engines.curation_engine.python.v1.x
            suffix = path.replace("lib.engines.curation.", "")
            return f"mda_platform.execution_plane.engines.curation_engine.python.{suffix}"
        if path.startswith("lib."):
            return "mda_platform.execution_plane." + path
        return path

    @staticmethod
    def resolve_and_validate(component_ref: dict, engine: str = "python") -> Callable:
        """
        Resolve component reference to executable function.
        
        Args:
            component_ref: Dict with 'path' and 'version' keys
                - path: Engine-relative (e.g., "v1.csv_parser.run") or 
                        fully qualified (e.g., "lib.engines.curation.v1.csv_parser.run")
                - version: Expected component version (e.g., "1.0.0")
            engine: Engine type from manifest evolution (e.g., "python", "python_spark")
            
        Returns:
            Callable function
            
        Raises:
            RuntimeError: If component cannot be resolved
            ValueError: If component metadata is missing or version mismatches
        """
        path = component_ref.get("path")
        expected_version = component_ref.get("version")
        
        # Determine if path is engine-relative or fully qualified
        if RuntimeResolver._is_engine_relative_path(path):
            # Engine-relative: expand based on engine type
            resolved_path = RuntimeResolver._expand_engine_relative_path(path, engine)
        else:
            # Fully qualified or legacy: remap if needed
            resolved_path = RuntimeResolver._remap_legacy_path(path)

        # 1. Resolve: Dynamic Import
        try:
            # Path points to module.function_name
            module_path = ".".join(resolved_path.split(".")[:-1])
            func_name = resolved_path.split(".")[-1]
            
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)
        except (ImportError, AttributeError) as e:
            raise RuntimeError(
                f"MDA RESOLUTION FAILURE: Could not find {path} "
                f"(resolved: {resolved_path}, engine: {engine}). Error: {e}"
            )

        # 2. Validate Identity: Check __mda_component__ metadata
        if not hasattr(func, "__mda_component__"):
            raise ValueError(f"MDA GOVERNANCE FAILURE: Tool {path} is missing __mda_component__ metadata.")

        metadata = getattr(func, "__mda_component__")
        actual_version = metadata.get("version")

        if actual_version != expected_version:
            raise ValueError(
                f"MDA VERSION MISMATCH: Manifest requested {expected_version}, "
                f"but museum tool at {path} is version {actual_version}."
            )

        logging.info(f"MDA RESOLVER: Successfully bound {path} (v{actual_version}) via engine '{engine}'")
        return func
