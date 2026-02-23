# MDA POC v2 - Manifest-Driven Architecture

A proof-of-concept implementation demonstrating a **Manifest-Driven Architecture** with a three-plane design:

- **Control Plane** - Manifest registry, store, and schema validation
- **Execution Plane** - Pluggable engines (Python, PySpark, DuckDB)
- **Storage Plane** - Wild, Raw, Fact, Semantic, Retrieval, and Evidence stores

## Features

- 🔄 **Engine Replaceability** - Swap execution engines without changing manifests
- 📋 **Manifest-Driven** - All pipelines defined declaratively via JSON/YAML manifests
- 🔍 **Full Traceability** - UTID-based tracking with Trace Everything Principle
- 🦆 **Multi-Engine Support** - Python, PySpark, and DuckDB engines included
- 🧠 **Multi-Layer Architecture** - Curation → Semantic → Retrieval pipeline
- 📊 **Evidence Store** - Complete audit trail with Bill of Materials (BOM)

## Prerequisites

### Python 3.11+
```bash
# Check your Python version
python3 --version
```

### uv (Python Package Manager)
```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Java 17+ (Required for PySpark engine)

#### macOS (Homebrew)
```bash
# Install OpenJDK 17
brew install openjdk@17

# Add to your shell profile (~/.zshrc or ~/.bashrc)
echo 'export JAVA_HOME="/opt/homebrew/opt/openjdk@17"' >> ~/.zshrc
echo 'export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Verify installation
java -version
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

#### Windows
Download and install from [Adoptium](https://adoptium.net/temurin/releases/?version=17)

> **Note**: If you don't install Java, the PySpark engine will gracefully fall back to Python-based processing.

## Installation

```bash
# Clone the repository
git clone https://github.com/USAFacts/poc-manifest-driven-architecture.git

# Install dependencies (creates .venv automatically)
uv sync

# Verify installation
uv run python --version
```

## Quick Start

```bash
# Reset the platform (clears all stores)
make reset

# Run a complete pipeline (Curation → Semantic → Retrieval)
make go bls_employment_stats v1.0.0
make semantic bls_employment_ontology
make retrieve employment_analysis

# Trace full lineage back to source
make trace latest retrieval
```

## 🎯 Demonstration Scenarios (10 Demos)

Run the interactive demo runner:
```bash
make demo              # Run all demos interactively
make demo list         # List all demos
make demo 1            # Run specific demo
make demo 1-3          # Run demo range
make demo auto         # Run all without pauses
```

### Demo Overview

| # | Demo | Manifest | Key Capability |
|---|------|----------|----------------|
| 1 | First Curation Manifest | BLS Employment Stats v1.0.0 | Parser & schema auto-onboarding |
| 2 | Another Agency | Census Population v1.0.0 | Multi-agency support |
| 3 | Added Capability | Census Population v1.1.0 | Field mapping + state enrichment |
| 4 | Updated Capability | Census Population v1.2.0 | Upgraded csv_parser (v2 lowercase) |
| 5 | New Engine: Spark | Census Population v2.0.0 | Engine pluggability (PySpark) |
| 6 | New Engine: DuckDB | Census Population v3.0.0 | Engine pluggability (SQL-based) |
| 7 | Changed Manifest Schema | BLS Employment Stats v2.0.0 | Schema evolution (v1 → v2) |
| 8 | Semantic Projection | BLS Employment Ontology v1.0.0 | Curation → Semantic layer |
| 9 | Retrieval Synthesis | Employment Analysis v1.0.0 | Multi-source retrieval |
| 10 | Replay Execution | Census Population v1.0.0 (replay) | Audit trail & idempotency |

---

### Demo 1: First Curation Manifest
**BLS Employment Stats v1.0.0**

The FIRST manifest onboarded to the platform. Demonstrates automatic onboarding of:
- Parser registry (`parser_registry.py`)
- Manifest parser v1
- Manifest schema v1.0.0

```bash
make reset
make go bls_employment_stats v1.0.0
```

**What to observe:**
- 🏗️ Parser registry onboarded (first manifest)
- 🔧 Parser v1 onboarded for curation
- 📐 Schema v1.0.0 onboarded
- ✅ EXECUTION COMPLETE with BOM

---

### Demo 2: Another Agency
**Census Population v1.0.0**

Adding a SECOND AGENCY demonstrates multi-tenancy. Same schema version, different agency and data model.

```bash
make go census/census_population v1.0.0
```

**What to observe:**
- Different agency folder structure
- Same parser version reused
- Agency-specific data model applied

---

### Demo 3: Added Capability
**Census Population v1.1.0**

Adding NEW PROCESSING STEPS: field mapping and state enrichment.

```bash
make go census/census_population v1.1.0
```

**What to observe:**
- New step: `map_fields` — renames `pop` → `population`
- New step: `enrich_state` — adds `state_code` from reference data
- Backward compatible change (additive capabilities)

---

### Demo 4: Updated Capability
**Census Population v1.2.0**

UPGRADING the csv_parser component from v1 to v2 (converts strings to lowercase).

```bash
make go census/census_population v1.2.0
```

**What to observe:**
- `csv_parser` upgraded: v1.0.0 → v2.0.0 (lowercase conversion)
- Same mapping & enrichment capabilities from v1.1.0
- Component versioning: same step, different implementation

---

### Demo 5: New Engine - Python + Spark
**Census Population v2.0.0**

Switching to a DIFFERENT EXECUTION ENGINE (PySpark) via major version bump.

```bash
make go census/census_population v2.0.0
```

**What to observe:**
- Engine: `python_spark v1.0.0`
- 🔥 Spark indicators in output
- YAML manifest format (V2 schema)

> Requires Java 17+. Falls back gracefully if Java not installed.

---

### Demo 6: New Engine - Python + DuckDB
**Census Population v3.0.0**

Using DUCKDB for SQL-based analytical processing.

```bash
make go census/census_population v3.0.0
```

**What to observe:**
- Engine: `python_duckdb v1.0.0`
- 🦆 DuckDB indicators in output
- SQL-based quality checks

---

### Demo 7: Changed Manifest Schema
**BLS Employment Stats v2.0.0**

MANIFEST SCHEMA EVOLUTION from v1 to v2 format.

```bash
make go bls_employment_stats v2.0.0
```

**What to observe:**
- 🔧 Parser v2 onboarded
- 📐 Schema v2.0.0 onboarded
- YAML format with `manifest:` wrapper

---

### Demo 8: Semantic Projection
**BLS Employment Ontology v1.0.0**

The SEMANTIC LAYER projects curation facts into semantic concepts.

```bash
make reset && make go bls_employment_stats v1.0.0
make semantic bls_employment_ontology
```

**What to observe:**
- 📥 SOURCE BIND PHASE
- Bound to Curation UTID
- Ontology mapper component
- `doc_id` inherited from curation source

---

### Demo 9: Retrieval Synthesis
**Employment Analysis v1.0.0**

The RETRIEVAL LAYER joins semantic projections to answer questions.

```bash
make retrieve employment_analysis
make trace latest retrieval
```

**What to observe:**
- 📥 FETCH DEPENDENCIES PHASE
- Bound Semantic UTIDs
- Synthesis/Join phase
- ✅ TRACE VERIFIED - Full lineage to source

---

### Demo 10: Replay Execution
**Census Population v1.0.0 (replay)**

REPLAY demonstrates audit trail preservation. Each execution creates a new UTID.

```bash
make list evidence                        # Before: count evidence files
make go census/census_population v1.0.0   # Replay
make list evidence                        # After: new evidence file
make list fact                            # New fact file with unique UTID
```

**What to observe:**
- New UTID minted (compare to previous)
- TWO evidence files for census_population
- TWO fact files for census_population
- Full audit trail preserved

---

## Lineage Tracing

The **Trace Everything Principle** ensures every retrieval answer is traceable to source documents.

```bash
# Trace specific UTID
make trace utid-xxxxxxxx

# Trace latest retrieval
make trace latest retrieval

# Trace latest semantic
make trace latest semantic
```

Example output:
```
├── 🔍 RETRIEVAL: employment_analysis v1.0.0
│     UTID: utid-59ff77a1-...
│     doc_ids: ['doc-7f429aad-...']
  ├── 🧠 SEMANTIC: bls_employment_ontology v1.0.0
  │     UTID: utid-a8a9fe75-...
  │     doc_id: doc-7f429aad-...
  │     ↓ curation_utid: utid-07c9041a-...
    ├── 📦 CURATION: bls_employment_stats v1.0.0
    │     UTID: utid-07c9041a-...
    │     doc_id: doc-7f429aad-...
    │     Wild Source: wild/employment_stats.csv
    │     Raw Doc: raw/raw-0001-utid-07c9041a-..._bls_employment_stats_v1.0.0.json

✅ TRACE VERIFIED - Source document(s): ['doc-7f429aad-...']
```

## Available Commands

```bash
make help                           # Show all commands

# Lifecycle
make reset                          # Reset platform (clear all stores)
make go <manifest> [version]        # Full pipeline: onboard → deploy → trigger
make semantic <manifest>            # Run semantic projection
make retrieve <manifest>            # Run retrieval

# Tracing & Demo
make trace <utid>                   # Trace lineage by UTID
make trace latest [type]            # Trace latest execution
make demo                           # Run interactive demos
make demo list                      # List all demos

# Inspection
make list platform                  # Platform directory structure
make list registry [layer]          # Registered manifests
make list manifests [layer]         # Deployed manifests
make list fact [agency]             # Fact store contents
make list evidence                  # Evidence store contents
make show <manifest> [version]      # Show manifest content
```

## Project Structure

```
poc-manifest-driven-architecture/
├── mda_platform/             # Core platform implementation
│   ├── control_plane/        # Registry, Manifest Store, Orchestrator
│   │   ├── registry/         # Schemas, parsers, data models
│   │   ├── manifest_store/   # Deployed manifests (versioned)
│   │   └── orchestrator/     # UTID minting, dispatch
│   ├── execution_plane/      # Pluggable engines
│   │   ├── engines/          # Curation, Semantic, Retrieval engines
│   │   └── common/           # Shared connectors (evidence store)
│   └── storage_plane/        # All data stores
│       ├── wild/             # Source files (landing zone)
│       ├── raw/              # Ingested raw data
│       ├── fact_store/       # Curated facts
│       ├── semantic_store/   # Semantic projections
│       ├── retrieval_store/  # Retrieval outputs
│       └── evidence_store/   # Audit trail (BOM)
├── staging/                  # Staging area for new manifests (not yet onboarded)
│   ├── curation/manifests/   # Curation manifests (BLS, Census)
│   ├── semantics/manifests/  # Semantic manifests (ontologies)
│   └── retrieval/manifests/  # Retrieval manifests (analyses)
├── onboard.py                # Manifest onboarding script
├── deploy.py                 # Manifest deployment script
├── trigger.py                # Pipeline trigger script
├── trace.py                  # Lineage tracing tool
├── demo.py                   # Demo runner (10 scenarios)
├── Makefile                  # Build/run commands
└── pyproject.toml            # Python dependencies
```

## Engines

| Engine | Manifest Version | Description | Requirements |
|--------|------------------|-------------|--------------|
| `python` | v1.0.0, v1.1.0, v1.2.0 | Standard Python processing | None |
| `python_spark` | v2.0.0 | PySpark DataFrame operations | Java 17+ |
| `python_duckdb` | v3.0.0 | DuckDB SQL processing | None |

Each engine demonstrates different processing approaches while maintaining the same manifest interface.

## Example Manifests

### Curation Manifest (v1 JSON)
```json
{
  "manifest_id": "census_population",
  "version": "1.0.0",
  "layer": "curation",
  "agency": "census",
  "engine": "python",
  "source": {
    "type": "csv",
    "path": "census/population.csv"
  },
  "processing": {
    "steps": [
      {"component": "parse_csv", "version": "1.0.0"},
      {"component": "validate_quality", "version": "1.0.0"},
      {"component": "write_facts", "version": "1.0.0"}
    ]
  }
}
```

### Semantic Manifest (YAML)
```yaml
manifest:
  manifest_id: bls_employment_ontology
  version: "1.0.0"
  layer: semantics
  domain: macroeconomics
  
  source:
    type: curation_ref
    manifest_id: bls_employment_stats
    
  projection:
    ontology: employment_ontology
    mappings:
      - physical: employment_rate
        semantic: EmploymentMetric
```

### Retrieval Manifest (YAML)
```yaml
manifest:
  manifest_id: employment_analysis
  version: "1.0.0"
  layer: retrieval
  domain: macroeconomics
  
  sources:
    primary:
      type: semantic_ref
      manifest_id: bls_employment_ontology
      
  synthesis:
    type: temporal_join
    output_format: jsonld
```

## Troubleshooting

### PySpark: "Unable to locate a Java Runtime"
Install Java 17+ and set JAVA_HOME:
```bash
# macOS
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
```

### DuckDB: Import errors
Ensure pandas is installed:
```bash
uv sync  # Reinstalls all dependencies
```

### Module not found errors
Make sure you're running from the project root with `uv run`:
```bash
cd mda-poc-v2
uv run trigger.py census_population
```

### Trace shows "unknown" values
Run a full pipeline first to generate evidence:
```bash
make reset
make go bls_employment_stats v1.0.0
make semantic bls_employment_ontology
make retrieve employment_analysis
make trace latest retrieval
```

## MDA Component Definition

An **MDA Component** is a versioned, reusable execution unit with the following characteristics:

### Structure
```python
def run(ctx: dict, params: dict) -> str:
    """
    Execute the component logic.
    
    Args:
        ctx: Execution context (utid, manifest, doc_id, etc.)
        params: Component parameters from manifest
        
    Returns:
        Status message (e.g., "PARSE_SUCCESS: 5 rows x 4 cols")
    """
    utid = ctx["utid"]
    # ... processing logic ...
    return "SUCCESS: description"

# MDA Component Metadata (required)
run.__mda_component__ = {
    "version": "1.0.0",
    "interface": "mda.interfaces.parse.v1"
}
```

### Location
Components live in the Execution Plane under versioned directories:
```
mda_platform/execution_plane/engines/{engine_type}/{layer}/v{N}/{component}.py
```

### Key Characteristics
| Property | Description |
|----------|-------------|
| **Versioned** | Multiple versions coexist (`v1/`, `v2/`) |
| **Stateless** | All state from `ctx` and `params` |
| **Traceable** | Recorded in BOM with path and version |
| **Manifest-driven** | Referenced by path in manifest `intent.processing[]` |

### Examples
| Component | Purpose |
|-----------|---------|
| `v1.ingest_default` | Wild → Raw ingestion |
| `v1.csv_parser` / `v2.csv_parser` | Parse CSV (v2 adds lowercase) |
| `v1.field_mapper` | Rename columns |
| `v1.enrich_state` | Add reference data |
| `v1.validate_quality` | Data quality checks |
| `v1.fact_store_writer` | Write to fact store |

---

## Architecture Principles

1. **Manifest-Driven** - No hardcoded pipeline logic; all behavior from manifests
2. **Engine Pluggability** - Same manifest, different runtime (Python/Spark/DuckDB)
3. **Schema Evolution** - Parsers auto-onboard on first use of new schema version
4. **Trace Everything** - Every retrieval answer traceable to source document(s)
5. **Immutable Evidence** - Each execution creates new UTID; audit trail preserved

