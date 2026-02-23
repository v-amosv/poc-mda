#!/usr/bin/env python3
# demo.py
"""
MDA POC Demo Runner

Orchestrates the 10 demonstration scenarios with explanations and pauses.
Each demo showcases a specific MDA capability.

Usage:
    uv run demo.py                  # Run all demos interactively
    uv run demo.py --demo 1         # Run specific demo
    uv run demo.py --demo 1-3       # Run demo range
    uv run demo.py --auto           # Run all without pauses
    uv run demo.py --list           # List all demos

Demos:
    1. First curation manifest (BLS v1.0.0)
    2. Another agency (Census v1.0.0)
    3. Added capability (Census v1.1.0)
    4. Updated capability (Census v1.2.0)
    5. New engine: python_spark (Census v2.0.0)
    6. New engine: python_duckdb (Census v3.0.0)
    7. Changed manifest schema (BLS v2.0.0)
    8. Semantic projection (BLS Ontology v1.0.0)
    9. Retrieval synthesis (Employment Analysis v1.0.0)
   10. Replay execution (Census v1.0.0 replay)
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).parent

# Demo definitions: (title, description, commands, what_to_observe)
DEMOS = [
    {
        "num": 1,
        "title": "First Curation Manifest",
        "manifest": "BLS Employment Stats v1.0.0",
        "description": """
This demonstrates the FIRST manifest being onboarded to the platform.
The parser registry and schema are onboarded dynamically on first use.

Key Points:
  • Staging → Registry → Manifest Store → Execution
  • Parser v1 and Schema v1.0.0 auto-onboarded
  • Full Bill of Materials (BOM) recorded in Evidence Store
  • doc_id generated for source traceability
""",
        "commands": [
            ("Reset platform", "make reset"),
            ("Onboard, Deploy, Execute", "make go bls_employment_stats v1.0.0"),
        ],
        "observe": [
            "🏗️ Parser registry onboarded (first manifest)",
            "🔧 Parser v1 onboarded for curation",
            "📐 Schema v1.0.0 onboarded",
            "✅ EXECUTION COMPLETE with BOM",
        ],
    },
    {
        "num": 2,
        "title": "Another Agency",
        "manifest": "Census Population v1.0.0",
        "description": """
This demonstrates adding a SECOND AGENCY to the platform.
The Census manifest uses the same v1 schema but different data model.

Key Points:
  • Same schema version (v1.0.0), different agency
  • Agency-specific data_model and reference_data
  • Proves multi-agency support
""",
        "commands": [
            ("Onboard Census manifest", "make go census/census_population v1.0.0"),
        ],
        "observe": [
            "Different agency folder structure",
            "Same parser version reused",
            "Agency-specific data model applied",
        ],
    },
    {
        "num": 3,
        "title": "Added Capability",
        "manifest": "Census Population v1.1.0",
        "description": """
This demonstrates adding a NEW PROCESSING STEP to an existing manifest.
v1.1.0 adds quality validation without changing ingestion or parsing.

Key Points:
  • Minor version bump (1.0.0 → 1.1.0)
  • Backward compatible change
  • Same parser handles both versions
""",
        "commands": [
            ("Execute updated manifest", "make go census/census_population v1.1.0"),
        ],
        "observe": [
            "New processing step: validate_quality",
            "Quality checks reported in output",
            "Same schema version, new capability",
        ],
    },
    {
        "num": 4,
        "title": "Updated Capability",
        "manifest": "Census Population v1.2.0",
        "description": """
This demonstrates UPDATING an existing processing step.
v1.2.0 modifies the validation parameters for stricter checks.

Key Points:
  • Patch version bump (1.1.0 → 1.2.0)
  • Configuration change, not code change
  • Manifest-driven behavior modification
""",
        "commands": [
            ("Execute with updated params", "make go census/census_population v1.2.0"),
        ],
        "observe": [
            "Different validation parameters",
            "Same components, different configuration",
            "Version history preserved in manifest store",
        ],
    },
    {
        "num": 5,
        "title": "New Engine: Python + Spark",
        "manifest": "Census Population v2.0.0",
        "description": """
This demonstrates switching to a DIFFERENT EXECUTION ENGINE.
v2.0.0 uses python_spark for distributed processing.

Key Points:
  • Major version bump (1.x → 2.0.0)
  • Engine field: "python_spark"
  • Same manifest schema, different runtime
  • YAML manifest format (V2 schema)
""",
        "commands": [
            ("Execute with Spark engine", "make go census/census_population v2.0.0"),
        ],
        "observe": [
            "Engine: python_spark v1.0.0",
            "🔥 Spark indicators in output",
            "YAML manifest format",
        ],
    },
    {
        "num": 6,
        "title": "New Engine: Python + DuckDB",
        "manifest": "Census Population v3.0.0",
        "description": """
This demonstrates the DUCKDB execution engine.
v3.0.0 uses python_duckdb for analytical SQL processing.

Key Points:
  • Engine field: "python_duckdb"
  • SQL-based validation (not Python loops)
  • Demonstrates engine pluggability
""",
        "commands": [
            ("Execute with DuckDB engine", "make go census/census_population v3.0.0"),
        ],
        "observe": [
            "Engine: python_duckdb v1.0.0",
            "🦆 DuckDB indicators in output",
            "SQL-based quality checks",
        ],
    },
    {
        "num": 7,
        "title": "Changed Manifest Schema",
        "manifest": "BLS Employment Stats v2.0.0",
        "description": """
This demonstrates a MANIFEST SCHEMA EVOLUTION.
v2.0.0 uses manifest_schema_version 2.0.0 with YAML format.

Key Points:
  • New manifest schema (2.0.0)
  • YAML format with 'manifest:' wrapper
  • V2 parser auto-onboarded on first use
  • Backward compatibility maintained
""",
        "commands": [
            ("Execute V2 schema manifest", "make go bls_employment_stats v2.0.0"),
        ],
        "observe": [
            "🔧 Parser v2 onboarded",
            "📐 Schema v2.0.0 onboarded",
            "YAML format accepted",
        ],
    },
    {
        "num": 8,
        "title": "Semantic Projection",
        "manifest": "BLS Employment Ontology v1.0.0",
        "description": """
This demonstrates the SEMANTIC LAYER.
The ontology manifest projects curation facts into semantic concepts.

Key Points:
  • Semantic engine (different from curation)
  • Source binding to curation UTID
  • Ontology mapping (physical → semantic)
  • doc_id inherited from source curation
""",
        "commands": [
            ("Reset and run curation first", "make reset && make go bls_employment_stats v1.0.0"),
            ("Execute semantic projection", "make semantic bls_employment_ontology"),
        ],
        "observe": [
            "📥 SOURCE BIND PHASE",
            "Bound to Curation UTID",
            "Ontology mapper component",
            "doc_id matches curation source",
        ],
    },
    {
        "num": 9,
        "title": "Retrieval Synthesis",
        "manifest": "Employment Analysis v1.0.0",
        "description": """
This demonstrates the RETRIEVAL LAYER.
The retrieval manifest joins semantic projections to answer questions.

Key Points:
  • Fan-in pattern (multiple sources)
  • Cross-domain synthesis
  • UTID chain: Retrieval → Semantic → Curation
  • doc_ids collected from ALL sources (Trace Everything)
""",
        "commands": [
            ("Execute retrieval", "make retrieve employment_analysis"),
            ("Trace lineage", "uv run trace.py --latest retrieval"),
        ],
        "observe": [
            "📥 FETCH DEPENDENCIES PHASE",
            "Bound Semantic UTIDs",
            "Synthesis/Join phase",
            "✅ TRACE VERIFIED - Full lineage to source",
        ],
    },
    {
        "num": 10,
        "title": "Replay Execution",
        "manifest": "Census Population v1.0.0 (replay)",
        "description": """
This demonstrates REPLAY - re-executing an already-deployed manifest.
The platform generates a NEW UTID for each execution, preserving full audit trail.

Key Points:
  • Same manifest, new execution
  • New UTID minted (different from first run)
  • New evidence record created (audit trail)
  • New fact record in fact store

After Execution, Verify:
  → Evidence Store: make list evidence
    - Look for TWO curation evidence files for census_population
  → Fact Store: make list fact
    - Look for TWO fact files for census_population
  → Each execution is independently traceable!
""",
        "commands": [
            ("List evidence before replay", "make list evidence"),
            ("Replay Census v1.0.0", "make go census/census_population v1.0.0"),
            ("List evidence after replay", "make list evidence"),
            ("List facts after replay", "make list fact"),
        ],
        "observe": [
            "New UTID minted (compare to previous)",
            "TWO evidence files for census_population",
            "TWO fact files for census_population",
            "Full audit trail preserved",
        ],
    },
]


def print_header(text: str, char: str = "="):
    """Print a header with decorators."""
    width = 70
    print()
    print(char * width)
    print(f"  {text}")
    print(char * width)


def print_demo_intro(demo: dict):
    """Print demo introduction."""
    print_header(f"DEMO #{demo['num']}: {demo['title']}", "═")
    print(f"\n  📋 Manifest: {demo['manifest']}")
    print(demo['description'])
    
    print("  What to observe:")
    for item in demo['observe']:
        print(f"    • {item}")
    print()


def run_command(description: str, command: str, auto: bool = False) -> bool:
    """Run a shell command with description."""
    print(f"\n  ▶ {description}")
    print(f"    $ {command}")
    
    if not auto:
        input("    [Press Enter to execute...]")
    
    result = subprocess.run(
        command,
        shell=True,
        cwd=PROJECT_ROOT,
        capture_output=False
    )
    
    return result.returncode == 0


def run_demo(demo: dict, auto: bool = False) -> bool:
    """Run a single demo."""
    print_demo_intro(demo)
    
    if not auto:
        response = input("  Ready to run this demo? [Y/n/skip] ").strip().lower()
        if response == 'skip' or response == 's':
            print("  ⏭️  Skipped")
            return True
        if response == 'n':
            print("  ❌ Cancelled")
            return False
    
    for desc, cmd in demo['commands']:
        success = run_command(desc, cmd, auto)
        if not success:
            print(f"\n  ⚠️  Command failed, continuing...")
    
    print_header(f"Demo #{demo['num']} Complete", "─")
    
    if not auto:
        input("\n  [Press Enter for next demo...]")
    
    return True


def parse_demo_range(range_str: str) -> List[int]:
    """Parse demo range like '1', '1-3', '1,3,5'."""
    demos = []
    for part in range_str.split(','):
        if '-' in part:
            start, end = part.split('-')
            demos.extend(range(int(start), int(end) + 1))
        else:
            demos.append(int(part))
    return sorted(set(demos))


def list_demos():
    """List all available demos."""
    print_header("MDA POC Demonstration Scenarios")
    print()
    for demo in DEMOS:
        print(f"  Demo #{demo['num']}: {demo['title']}")
        print(f"          {demo['manifest']}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="MDA POC Demo Runner - 9 Demonstration Scenarios",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    uv run demo.py                  Run all demos interactively
    uv run demo.py --demo 1         Run only demo #1
    uv run demo.py --demo 1-3       Run demos 1 through 3
    uv run demo.py --demo 8,9       Run demos 8 and 9
    uv run demo.py --auto           Run all without pauses
    uv run demo.py --list           List all demos
        """
    )
    parser.add_argument("--demo", "-d", help="Demo number(s) to run (e.g., 1, 1-3, 1,3,5)")
    parser.add_argument("--auto", "-a", action="store_true", help="Run without pauses")
    parser.add_argument("--list", "-l", action="store_true", help="List all demos")
    
    args = parser.parse_args()
    
    if args.list:
        list_demos()
        return
    
    # Determine which demos to run
    if args.demo:
        demo_nums = parse_demo_range(args.demo)
        demos_to_run = [d for d in DEMOS if d['num'] in demo_nums]
    else:
        demos_to_run = DEMOS
    
    # Print intro
    print_header("MDA POC DEMONSTRATION", "█")
    print("""
  Manifest-Driven Architecture Proof of Concept
  
  This demonstration showcases:
    • Multi-layer execution (Curation → Semantic → Retrieval)
    • Multi-engine support (Python, Spark, DuckDB)
    • Schema evolution (v1 → v2)
    • Full lineage tracing (Trace Everything Principle)
    
  Each demo builds on the previous, showing progressive capabilities.
""")
    
    if not args.auto:
        response = input("  Start demonstration? [Y/n] ").strip().lower()
        if response == 'n':
            print("  Cancelled.")
            return
    
    # Run demos
    for demo in demos_to_run:
        success = run_demo(demo, args.auto)
        if not success:
            print("\n  Demonstration cancelled.")
            return
    
    # Final summary
    print_header("DEMONSTRATION COMPLETE", "█")
    print("""
  ✅ All demos executed successfully!
  
  Key Takeaways:
    1. Manifests drive ALL execution - no hardcoded logic
    2. Engines are pluggable - same manifest, different runtime
    3. Schemas evolve independently - parsers auto-onboard
    4. Every answer is traceable - Trace Everything Principle
    
  Run 'uv run trace.py --latest retrieval' to see full lineage.
""")


if __name__ == "__main__":
    main()
