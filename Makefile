# MDA POC v2 - Makefile
# ============================================================
# Usage:
#   make reset                              # Reset platform (force)
#   make go census_population               # Full lifecycle (latest version)
#   make go census_population v1.0.0        # Full lifecycle (specific version)
#   make list platform                      # Platform directory structure
#   make list registry                      # All registry manifests
#   make list registry curation             # Registry manifests in curation branch
#   make list manifests curation            # Manifest-store manifests in branch
#   make list wild|raw|fact|evidence        # All files in store
#   make list wild census                   # Files in store branch
#   make show census_population             # Show latest manifest
#   make show census_population v1.0.0      # Show specific version
#   make show fact census census_population # Show latest fact doc
# ============================================================

.PHONY: reset go list show help trace demo semantic retrieve full-pipeline latest curation retrieval

# Default target
help:
	@echo "============================================================"
	@echo "MDA POC v2 - Command Reference"
	@echo "============================================================"
	@echo ""
	@echo "  make reset                              Reset platform (force)"
	@echo ""
	@echo "  make go <manifest-id>                   Full lifecycle (latest)"
	@echo "  make go <manifest-id> v<version>        Full lifecycle (specific)"
	@echo ""
	@echo "  make semantic <manifest-id>             Run semantic projection"
	@echo "  make retrieve <manifest-id>             User-initiated retrieval"
	@echo "  make full-pipeline                      Complete demo pipeline"
	@echo ""
	@echo "  make trace <utid>                       Trace lineage by UTID"
	@echo "  make trace latest [type]                Trace latest evidence"
	@echo ""
	@echo "  make demo                               Run all demos"
	@echo "  make demo <n>                           Run specific demo"
	@echo "  make demo list                          List all demos"
	@echo ""
	@echo "  make list platform                      Platform directory tree"
	@echo "  make list registry [branch]             Registry manifests"
	@echo "  make list manifests [branch]            Manifest-store manifests"
	@echo "  make list wild|raw|fact|evidence [branch]"
	@echo ""
	@echo "  make show <manifest-id> [v<version>]    Show manifest"
	@echo "  make show <store> <branch> <manifest-id> Show store document"
	@echo "  make show deployments [manifest-id]     Show deployment history"
	@echo ""
	@echo "============================================================"

# ============================================================
# RESET - Force reset platform stores
# ============================================================
reset:
	@echo "============================================================"
	@echo "🔄 RESETTING PLATFORM (FORCE)"
	@echo "============================================================"
	@# Clear registry (manifests, schemas with parsers, data_model, reference_data)
	@rm -rf mda_platform/control_plane/registry/curation/manifests/*
	@rm -rf mda_platform/control_plane/registry/curation/schema/*
	@rm -rf mda_platform/control_plane/registry/semantics/manifests/*
	@rm -rf mda_platform/control_plane/registry/semantics/schema/*
	@rm -rf mda_platform/control_plane/registry/retrieval/manifests/*
	@rm -rf mda_platform/control_plane/registry/retrieval/schema/*
	@rm -rf mda_platform/control_plane/registry/silver/manifests/*
	@rm -rf mda_platform/control_plane/registry/gold/manifests/*
	@# Remove parser_registry.py from registry root (will be re-onboarded)
	@rm -f mda_platform/control_plane/registry/parser_registry.py
	@# Clear manifest store (all contents under store/)
	@rm -rf mda_platform/control_plane/manifest_store/store/curation/manifests/*
	@rm -rf mda_platform/control_plane/manifest_store/store/silver/manifests/*
	@rm -rf mda_platform/control_plane/manifest_store/store/gold/manifests/*
	@rm -rf mda_platform/control_plane/manifest_store/store/core/*
	@rm -rf mda_platform/control_plane/manifest_store/store/retrieval/*
	@rm -rf mda_platform/control_plane/manifest_store/store/semantics/*
	@# Clear storage plane
	@rm -rf mda_platform/storage_plane/wild/*
	@rm -rf mda_platform/storage_plane/raw/*
	@rm -rf mda_platform/storage_plane/fact_store/*
	@rm -rf mda_platform/storage_plane/evidence_store/*
	@rm -rf mda_platform/storage_plane/semantic_store/*
	@rm -rf mda_platform/storage_plane/retrieval_store/*
	@# Reset sequence files
	@rm -f mda_platform/storage_plane/raw/.seq 2>/dev/null || true
	@rm -f mda_platform/storage_plane/fact_store/.seq 2>/dev/null || true
	@rm -f mda_platform/storage_plane/semantic_store/.seq 2>/dev/null || true
	@rm -f mda_platform/storage_plane/retrieval_store/.seq 2>/dev/null || true
	@rm -f mda_platform/storage_plane/evidence_store/.seq 2>/dev/null || true
	@rm -f mda_platform/storage_plane/evidence_store/.seq_curation 2>/dev/null || true
	@rm -f mda_platform/storage_plane/evidence_store/.seq_semantic 2>/dev/null || true
	@rm -f mda_platform/storage_plane/evidence_store/.seq_retrieval 2>/dev/null || true
	@# Recreate directory structure
	@mkdir -p mda_platform/storage_plane/wild
	@mkdir -p mda_platform/storage_plane/raw
	@mkdir -p mda_platform/storage_plane/fact_store
	@mkdir -p mda_platform/storage_plane/evidence_store
	@mkdir -p mda_platform/storage_plane/semantic_store
	@mkdir -p mda_platform/storage_plane/retrieval_store
	@mkdir -p mda_platform/storage_plane/quarantine_store
	@# Restore .gitkeep files for all empty directories
	@for dir in \
		mda_platform/control_plane/manifest_store/store/curation/manifests \
		mda_platform/control_plane/manifest_store/store/curation/policy \
		mda_platform/control_plane/manifest_store/store/curation/schema \
		mda_platform/control_plane/manifest_store/store/retrieval/manifests \
		mda_platform/control_plane/manifest_store/store/retrieval/policy \
		mda_platform/control_plane/manifest_store/store/retrieval/schema \
		mda_platform/control_plane/manifest_store/store/semantics/manifests \
		mda_platform/control_plane/manifest_store/store/semantics/policy \
		mda_platform/control_plane/manifest_store/store/semantics/schema \
		mda_platform/control_plane/registry/curation/manifests \
		mda_platform/control_plane/registry/curation/policy \
		mda_platform/control_plane/registry/curation/schema \
		mda_platform/control_plane/registry/retrieval/manifests \
		mda_platform/control_plane/registry/retrieval/policy \
		mda_platform/control_plane/registry/retrieval/schema \
		mda_platform/control_plane/registry/semantics/manifests \
		mda_platform/control_plane/registry/semantics/policy \
		mda_platform/control_plane/registry/semantics/schema \
		mda_platform/storage_plane/evidence_store \
		mda_platform/storage_plane/fact_store \
		mda_platform/storage_plane/quarantine_store \
		mda_platform/storage_plane/raw \
		mda_platform/storage_plane/retrieval_store \
		mda_platform/storage_plane/semantic_store \
		mda_platform/storage_plane/wild; do \
		mkdir -p "$$dir" && \
		echo "# .gitkeep - Preserve empty directory structure" > "$$dir/.gitkeep"; \
	done
	@# Copy sample source data from staging to wild (POC only)
	@cp -r staging/sample_data/* mda_platform/storage_plane/wild/ 2>/dev/null || true
	@echo "✅ Platform reset complete"

# ============================================================
# GO - Full lifecycle: onboard → deploy → trigger
# ============================================================
# Usage: make go census_population
#        make go census_population v1.0.0
#        make go census/census_population
#        make go census/census_population v1.0.0
go:
	@if [ -z "$(word 2,$(MAKECMDGOALS))" ]; then \
		echo "❌ Usage: make go <manifest-id> [v<version>]"; \
		echo "         make go <agency>/<manifest-id> [v<version>]"; \
		exit 1; \
	fi
	@$(eval INPUT := $(word 2,$(MAKECMDGOALS)))
	@$(eval VERSION := $(word 3,$(MAKECMDGOALS)))
	@if echo "$(INPUT)" | grep -q "/"; then \
		AGENCY=$$(echo "$(INPUT)" | cut -d'/' -f1); \
		MANIFEST_ID=$$(echo "$(INPUT)" | cut -d'/' -f2); \
		FULL_ID="$$AGENCY/$$MANIFEST_ID"; \
	else \
		AGENCY="*"; \
		MANIFEST_ID="$(INPUT)"; \
		FULL_ID="$$MANIFEST_ID"; \
	fi; \
	if [ -z "$(VERSION)" ]; then \
		echo "🔍 Finding latest version for $$MANIFEST_ID..."; \
		LATEST=$$(ls -1 staging/curation/manifests/$$AGENCY/$${MANIFEST_ID}_v*.json staging/curation/manifests/$$AGENCY/$${MANIFEST_ID}_v*.yaml 2>/dev/null | sort -V | tail -1); \
		if [ -z "$$LATEST" ]; then \
			echo "❌ No manifest found for $$MANIFEST_ID"; \
			exit 1; \
		fi; \
		MANIFEST_PATH=$$(echo $$LATEST | sed 's|staging/||' | sed 's|/manifests/|/|' | sed 's|\.json||' | sed 's|\.yaml||'); \
		echo "📋 Latest: $$MANIFEST_PATH"; \
		uv run onboard.py $$MANIFEST_PATH && \
		uv run deploy.py $$MANIFEST_PATH && \
		uv run trigger.py $$FULL_ID; \
	else \
		VERSION_NUM=$$(echo $(VERSION) | sed 's/^v//'); \
		MANIFEST_PATH=$$(find staging/curation/manifests/$$AGENCY -maxdepth 1 \( -name "$${MANIFEST_ID}_v$$VERSION_NUM.json" -o -name "$${MANIFEST_ID}_v$$VERSION_NUM.yaml" \) 2>/dev/null | head -1); \
		if [ -z "$$MANIFEST_PATH" ]; then \
			echo "❌ Manifest $$MANIFEST_ID v$$VERSION_NUM not found"; \
			exit 1; \
		fi; \
		MANIFEST_PATH=$$(echo $$MANIFEST_PATH | sed 's|staging/||' | sed 's|/manifests/|/|' | sed 's|\.json||' | sed 's|\.yaml||'); \
		echo "📋 Using: $$MANIFEST_PATH"; \
		uv run onboard.py $$MANIFEST_PATH && \
		uv run deploy.py $$MANIFEST_PATH && \
		uv run trigger.py $$FULL_ID --version $$VERSION_NUM; \
	fi

# ============================================================
# SEMANTIC - Run semantic projection (full lifecycle)
# ============================================================
# Usage: make semantic bls_employment_ontology
semantic:
	@# Skip if called as argument to another target (e.g., make trace latest semantic)
	@FIRST_GOAL=$$(echo "$(MAKECMDGOALS)" | awk '{print $$1}'); \
	if [ "$$FIRST_GOAL" != "semantic" ]; then exit 0; fi; \
	if [ -z "$(word 2,$(MAKECMDGOALS))" ]; then \
		echo "❌ Usage: make semantic <manifest-id>"; \
		echo "   Example: make semantic bls_employment_ontology"; \
		exit 1; \
	fi; \
	MANIFEST_ID="$(word 2,$(MAKECMDGOALS))"; \
	echo "============================================================"; \
	echo "🧠 SEMANTIC PROJECTION LIFECYCLE"; \
	echo "============================================================"; \
	echo "  Manifest: $$MANIFEST_ID"; \
	echo ""; \
	MANIFEST_FILE=$$(find staging/semantics/manifests -name "$$MANIFEST_ID*.yaml" -o -name "$$MANIFEST_ID*.json" 2>/dev/null | head -1); \
	if [ -z "$$MANIFEST_FILE" ]; then \
		echo "❌ Semantic manifest '$$MANIFEST_ID' not found in staging/semantics/manifests/"; \
		exit 1; \
	fi; \
	DOMAIN=$$(dirname "$$MANIFEST_FILE" | xargs basename); \
	BASENAME=$$(basename "$$MANIFEST_FILE" | sed 's/\.[^.]*$$//'); \
	echo "📋 Found: semantics/$$DOMAIN/$$BASENAME"; \
	uv run onboard.py "semantics/$$DOMAIN/$$BASENAME" && \
	uv run deploy.py "semantics/$$DOMAIN/$$BASENAME" && \
	uv run trigger.py $$MANIFEST_ID --layer semantics

# ============================================================
# RETRIEVE - User-initiated retrieval (full lifecycle)
# ============================================================
# Usage: make retrieve employment_analysis
retrieve:
	@if [ -z "$(word 2,$(MAKECMDGOALS))" ]; then \
		echo "❌ Usage: make retrieve <manifest-id>"; \
		echo "   Example: make retrieve employment_analysis"; \
		exit 1; \
	fi
	@$(eval MANIFEST_ID := $(word 2,$(MAKECMDGOALS)))
	@echo "============================================================"
	@echo "📥 USER-INITIATED RETRIEVAL LIFECYCLE"
	@echo "============================================================"
	@echo "  Manifest: $(MANIFEST_ID)"
	@echo ""
	@# Find the retrieval manifest and onboard/deploy it
	@MANIFEST_FILE=$$(find staging/retrieval/manifests -name "$(MANIFEST_ID)*.yaml" -o -name "$(MANIFEST_ID)*.json" 2>/dev/null | head -1); \
	if [ -z "$$MANIFEST_FILE" ]; then \
		echo "❌ Retrieval manifest '$(MANIFEST_ID)' not found in staging/retrieval/manifests/"; \
		exit 1; \
	fi; \
	DOMAIN=$$(dirname "$$MANIFEST_FILE" | xargs basename); \
	BASENAME=$$(basename "$$MANIFEST_FILE" | sed 's/\.[^.]*$$//'); \
	echo "📋 Found: retrieval/$$DOMAIN/$$BASENAME"; \
	uv run onboard.py "retrieval/$$DOMAIN/$$BASENAME" && \
	uv run deploy.py "retrieval/$$DOMAIN/$$BASENAME" && \
	uv run retrieve.py $(MANIFEST_ID)

# ============================================================
# FULL-PIPELINE - Curation → Semantics → Retrieval
# ============================================================
# Usage: make full-pipeline
# Runs the complete demo pipeline with BLS data
full-pipeline:
	@echo "============================================================"
	@echo "🔄 FULL PIPELINE DEMONSTRATION"
	@echo "============================================================"
	@echo ""
	@echo "Step 1: Curation (bls_employment_stats)"
	@echo "────────────────────────────────────────"
	@make go bls_employment_stats
	@echo ""
	@echo "Step 2: Semantic Projection (bls_employment_ontology)"
	@echo "────────────────────────────────────────"
	@make semantic bls_employment_ontology
	@echo ""
	@echo "Step 3: Retrieval (employment_analysis)"
	@echo "────────────────────────────────────────"
	@make retrieve employment_analysis
	@echo ""
	@echo "============================================================"
	@echo "✅ FULL PIPELINE COMPLETE"
	@echo "============================================================"
	@echo ""
	@echo "Evidence chain created:"
	@echo "  Curation UTID → Semantic UTID → Retrieval UTID"
	@echo ""
	@ls -la mda_platform/storage_plane/evidence_store/ 2>/dev/null || echo "  (check evidence_store)"

# ============================================================
# LIST - List various structures and files
# ============================================================
list:
	@# Skip if called as argument to demo (e.g., make demo list)
	@if [ "$(word 1,$(MAKECMDGOALS))" = "demo" ]; then exit 0; fi; \
	TARGET="$(word 2,$(MAKECMDGOALS))"; \
	BRANCH="$(word 3,$(MAKECMDGOALS))"; \
	if [ -z "$$TARGET" ]; then \
		echo "❌ Usage: make list <target> [branch]"; \
		echo "   Targets: platform, registry, manifests, wild, raw, fact, evidence"; \
		exit 1; \
	fi; \
	case "$$TARGET" in \
		platform) \
			echo "============================================================"; \
			echo "📂 PLATFORM STRUCTURE"; \
			echo "============================================================"; \
			if command -v tree >/dev/null 2>&1; then \
				tree -d --noreport -I "__pycache__" mda_platform; \
			else \
				find mda_platform -type d -not -name "__pycache__" -not -path "*/__pycache__/*" | sort | awk ' \
					BEGIN { split("", last) } \
					{ \
						n = split($$0, parts, "/"); \
						for (i = 1; i < n; i++) { \
							if (last[i] != parts[i]) { \
								for (j = i; j <= length(last); j++) delete last[j]; \
							} \
						} \
						indent = ""; \
						for (i = 1; i < n; i++) { \
							if (i < n - 1) indent = indent "│   "; \
						} \
						if (n > 1) { \
							prefix = ""; \
							for (i = 1; i < n - 1; i++) prefix = prefix "│   "; \
							print prefix "├── " parts[n]; \
						} else { \
							print parts[n]; \
						} \
						for (i = 1; i <= n; i++) last[i] = parts[i]; \
					}'; \
			fi;; \
		registry) \
			echo "============================================================"; \
			echo "📋 REGISTRY MANIFESTS"; \
			echo "============================================================"; \
			if [ -z "$$BRANCH" ]; then \
				find mda_platform/control_plane/registry -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/control_plane/registry/[^/]*/manifests/||' || echo "  (empty)"; \
			else \
				find mda_platform/control_plane/registry/$$BRANCH -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/control_plane/registry/'$$BRANCH'/manifests/||' || echo "  (empty)"; \
			fi;; \
		manifests) \
			echo "============================================================"; \
			echo "📋 MANIFEST STORE"; \
			echo "============================================================"; \
			if [ -z "$$BRANCH" ]; then \
				find mda_platform/control_plane/manifest_store/store -name "*.json" -path "*/manifests/*" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/control_plane/manifest_store/store/[^/]*/manifests/||' || echo "  (empty)"; \
			else \
				find mda_platform/control_plane/manifest_store/store/$$BRANCH -name "*.json" -path "*/manifests/*" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/control_plane/manifest_store/store/'$$BRANCH'/manifests/||' || echo "  (empty)"; \
			fi;; \
		wild) \
			echo "============================================================"; \
			echo "🌿 WILD STORE"; \
			echo "============================================================"; \
			if [ -z "$$BRANCH" ]; then \
				find mda_platform/storage_plane/wild -type f ! -name ".*" 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/wild/||' || echo "  (empty)"; \
			else \
				find mda_platform/storage_plane/wild/$$BRANCH -type f ! -name ".*" 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/wild/'$$BRANCH'/||' || echo "  (empty)"; \
			fi;; \
		raw) \
			echo "============================================================"; \
			echo "📥 RAW STORE"; \
			echo "============================================================"; \
			if [ -z "$$BRANCH" ]; then \
				find mda_platform/storage_plane/raw -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/raw/||' || echo "  (empty)"; \
			else \
				find mda_platform/storage_plane/raw/$$BRANCH -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/raw/'$$BRANCH'/||' || echo "  (empty)"; \
			fi;; \
		fact) \
			echo "============================================================"; \
			echo "📊 FACT STORE"; \
			echo "============================================================"; \
			if [ -z "$$BRANCH" ]; then \
				find mda_platform/storage_plane/fact_store -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/fact_store/||' || echo "  (empty)"; \
			else \
				find mda_platform/storage_plane/fact_store/$$BRANCH -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/fact_store/'$$BRANCH'/||' || echo "  (empty)"; \
			fi;; \
		evidence) \
			echo "============================================================"; \
			echo "🔍 EVIDENCE STORE"; \
			echo "============================================================"; \
			if [ -z "$$BRANCH" ]; then \
				find mda_platform/storage_plane/evidence_store -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/evidence_store/||' || echo "  (empty)"; \
			else \
				find mda_platform/storage_plane/evidence_store/$$BRANCH -name "*.json" -type f 2>/dev/null | sort | \
					sed 's|mda_platform/storage_plane/evidence_store/'$$BRANCH'/||' || echo "  (empty)"; \
			fi;; \
		*) \
			echo "❌ Unknown target: $$TARGET"; \
			echo "   Valid targets: platform, registry, manifests, wild, raw, fact, evidence";; \
	esac

# ============================================================
# SHOW - Display manifest or store document
# ============================================================
show:
	@$(eval ARG1 := $(word 2,$(MAKECMDGOALS)))
	@$(eval ARG2 := $(word 3,$(MAKECMDGOALS)))
	@$(eval ARG3 := $(word 4,$(MAKECMDGOALS)))
	@if [ -z "$(ARG1)" ]; then \
		echo "❌ Usage:"; \
		echo "   make show <manifest-id> [v<version>]"; \
		echo "   make show <store> <branch> [file]"; \
		exit 1; \
	fi
	@case "$(ARG1)" in \
		deployments) \
			echo "============================================================"; \
			echo "📦 DEPLOYMENT HISTORY"; \
			echo "============================================================"; \
			STORE_PATH="mda_mda_platform/storage_plane/evidence_store"; \
			if [ -z "$(ARG2)" ]; then \
				FILES=$$(ls -1t $$STORE_PATH/deploy-*.json 2>/dev/null); \
			else \
				FILES=$$(ls -1t $$STORE_PATH/deploy-*-$(ARG2)-*.json 2>/dev/null); \
			fi; \
			if [ -z "$$FILES" ]; then \
				echo "  (no deployments found)"; \
			else \
				echo "$$FILES" | while read f; do \
					if [ -n "$$f" ]; then \
						MANIFEST_ID=$$(python3 -c "import json; print(json.load(open('$$f')).get('manifest_id', '?'))"); \
						VERSION=$$(python3 -c "import json; print(json.load(open('$$f')).get('version', '?'))"); \
						HASH=$$(python3 -c "import json; print(json.load(open('$$f')).get('content_hash', '?')[:12])"); \
						DEPLOYED=$$(python3 -c "import json; print(json.load(open('$$f')).get('deployed_at', '?')[:19])"); \
						STATUS=$$(python3 -c "import json; print(json.load(open('$$f')).get('status', '?'))"); \
						if [ "$$STATUS" = "SUCCESS" ]; then \
							echo "  ✅ $$MANIFEST_ID v$$VERSION"; \
						else \
							echo "  ❌ $$MANIFEST_ID v$$VERSION"; \
						fi; \
						echo "     Hash: $$HASH  Deployed: $$DEPLOYED"; \
					fi; \
				done; \
			fi;; \
		evidence) \
			STORE_PATH="mda_mda_platform/storage_plane/evidence_store"; \
			if [ -z "$(ARG2)" ]; then \
				LATEST=$$(ls -1t $$STORE_PATH/*.json 2>/dev/null | head -1); \
			else \
				LATEST=$$(ls -1t $$STORE_PATH/*$(ARG2)* 2>/dev/null | head -1); \
			fi; \
			if [ -z "$$LATEST" ]; then \
				echo "❌ No evidence document found"; \
				exit 1; \
			fi; \
			echo "============================================================"; \
			echo "📄 $$LATEST"; \
			echo "============================================================"; \
			cat "$$LATEST" | python3 -m json.tool 2>/dev/null || cat "$$LATEST";; \
		wild|raw|fact) \
			if [ -z "$(ARG2)" ]; then \
				echo "❌ Usage: make show $(ARG1) <branch> [file]"; \
				exit 1; \
			fi; \
			case "$(ARG1)" in \
				wild) STORE_PATH="mda_mda_platform/storage_plane/wild/$(ARG2)";; \
				raw) STORE_PATH="mda_mda_platform/storage_plane/raw/$(ARG2)";; \
				fact) STORE_PATH="mda_mda_platform/storage_plane/fact_store/$(ARG2)";; \
			esac; \
			if [ -z "$(ARG3)" ]; then \
				LATEST=$$(ls -1t $$STORE_PATH/* 2>/dev/null | head -1); \
			else \
				LATEST=$$(ls -1t $$STORE_PATH/*$(ARG3)* 2>/dev/null | head -1); \
			fi; \
			if [ -z "$$LATEST" ]; then \
				echo "❌ No $(ARG1) document found in $(ARG2)"; \
				exit 1; \
			fi; \
			echo "============================================================"; \
			echo "📄 $$LATEST"; \
			echo "============================================================"; \
			cat "$$LATEST" | python3 -m json.tool 2>/dev/null || cat "$$LATEST";; \
		*) \
			MANIFEST_ID="$(ARG1)"; \
			VERSION="$(ARG2)"; \
			if [ -z "$$VERSION" ]; then \
				LATEST_PTR=$$(find mda_platform/control_plane/manifest_store -path "*/$${MANIFEST_ID}/_latest.json" 2>/dev/null | head -1); \
				if [ -n "$$LATEST_PTR" ]; then \
					LATEST_VER=$$(python3 -c "import json; print(json.load(open('$$LATEST_PTR'))['version'])"); \
					LATEST=$$(find mda_platform/control_plane/manifest_store -path "*/$${MANIFEST_ID}/v$${LATEST_VER}/manifest.json" 2>/dev/null | head -1); \
				fi; \
			else \
				VERSION_NUM=$$(echo $$VERSION | sed 's/^v//'); \
				LATEST=$$(find mda_platform/control_plane/manifest_store -path "*/$${MANIFEST_ID}/v$${VERSION_NUM}/manifest.json" 2>/dev/null | head -1); \
			fi; \
			if [ -z "$$LATEST" ]; then \
				echo "❌ Manifest $$MANIFEST_ID not found in manifest store"; \
				exit 1; \
			fi; \
			echo "============================================================"; \
			echo "📄 $$LATEST"; \
			echo "============================================================"; \
			cat "$$LATEST" | python3 -m json.tool;; \
	esac

# ============================================================
# TRACE - Lineage tracing (Trace Everything Principle)
# ============================================================
# Usage: make trace utid-xxxxxxxx       Trace specific UTID
#        make trace latest retrieval    Trace latest retrieval
trace:
	@if [ -z "$(word 2,$(MAKECMDGOALS))" ]; then \
		echo "❌ Usage: make trace <utid>"; \
		echo "         make trace latest [curation|semantic|retrieval]"; \
		exit 1; \
	fi
	@$(eval ARG1 := $(word 2,$(MAKECMDGOALS)))
	@$(eval ARG2 := $(word 3,$(MAKECMDGOALS)))
	@if [ "$(ARG1)" = "latest" ]; then \
		if [ -z "$(ARG2)" ]; then \
			uv run trace.py --latest; \
		else \
			uv run trace.py --latest $(ARG2); \
		fi; \
	else \
		uv run trace.py $(ARG1); \
	fi

# ============================================================
# DEMO - Run demonstration scenarios
# ============================================================
# Usage: make demo              Run all demos interactively
#        make demo 1            Run demo #1
#        make demo 1-3          Run demos 1-3
#        make demo auto         Run all without pauses
#        make demo list         List demos
demo:
	@$(eval ARG1 := $(word 2,$(MAKECMDGOALS)))
	@if [ -z "$(ARG1)" ]; then \
		uv run demo.py; \
	elif [ "$(ARG1)" = "auto" ]; then \
		uv run demo.py --auto; \
	elif [ "$(ARG1)" = "list" ]; then \
		uv run demo.py --list; \
	else \
		uv run demo.py --demo $(ARG1); \
	fi

# Catch-all to prevent "No rule to make target" errors for arguments
%:
	@:
