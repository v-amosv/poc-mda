#!/usr/bin/env python3
# reset.py
"""
MDA POC Reset Script

Clears all execution artifacts to return to pre-onboarding state:
- Evidence Store (execution records)
- Raw Store (ingested files)
- Fact Store (curated output)
- Quarantine Store (failed data)
- Registry manifests (PR-ready manifests)
- Manifest Store manifests (deployed manifests)

Does NOT clear:
- Staging (source manifests for demos)
- Wild (source data)
- Reference data (deployed lookups)

Usage:
    uv run reset.py              # Interactive confirmation
    uv run reset.py --force      # No confirmation
    uv run reset.py --dry-run    # Show what would be deleted
"""
import argparse
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
STORAGE_PLANE = PROJECT_ROOT / "mda_platform" / "storage_plane"
CONTROL_PLANE = PROJECT_ROOT / "mda_platform" / "control_plane"

sys.path.insert(0, str(PROJECT_ROOT))


def get_reset_dirs() -> list:
    """Dynamically discover directories to reset.
    
    Returns list of tuples: (path, remove_dir)
    - remove_dir=True: Delete the entire directory (e.g., agency folders in registry)
    - remove_dir=False: Delete contents only, keep directory structure
    """
    dirs = [
        (STORAGE_PLANE / "evidence_store", False),
        (STORAGE_PLANE / "quarantine_store", False),
    ]
    
    # Add all agency folders under raw/ (remove entire agency folder)
    raw_dir = STORAGE_PLANE / "raw"
    if raw_dir.exists():
        for agency in raw_dir.iterdir():
            if agency.is_dir():
                dirs.append((agency, True))
    
    # Add all agency folders under fact_store/ (remove entire agency folder)
    fact_dir = STORAGE_PLANE / "fact_store"
    if fact_dir.exists():
        for agency in fact_dir.iterdir():
            if agency.is_dir():
                dirs.append((agency, True))
    
    # Add all agency folders under registry/curation/manifests/ (remove entire folder)
    registry_manifests = CONTROL_PLANE / "registry" / "curation" / "manifests"
    if registry_manifests.exists():
        for agency in registry_manifests.iterdir():
            if agency.is_dir():
                dirs.append((agency, True))
    
    # Add all agency folders under manifest_store/store/curation/manifests/ (remove entire folder)
    store_manifests = CONTROL_PLANE / "manifest_store" / "store" / "curation" / "manifests"
    if store_manifests.exists():
        for agency in store_manifests.iterdir():
            if agency.is_dir():
                dirs.append((agency, True))
    
    return dirs


def reset_sequences() -> None:
    """Reset all sequence counters."""
    from mda_platform.execution_plane.common.connectors.sequence_counter import reset_all_sequences
    reset_all_sequences()


def count_files(directory: Path) -> int:
    """Count files in a directory."""
    if not directory.exists():
        return 0
    return sum(1 for f in directory.rglob("*") if f.is_file())


def reset(dry_run: bool = False) -> dict:
    """Reset all execution artifacts."""
    stats = {"deleted": 0, "directories": []}
    
    reset_dirs = get_reset_dirs()
    
    for dir_path, remove_dir in reset_dirs:
        if not dir_path.exists():
            continue
        
        file_count = count_files(dir_path)
        if file_count == 0 and not remove_dir:
            continue
        
        stats["directories"].append({
            "path": str(dir_path.relative_to(PROJECT_ROOT)),
            "files": file_count,
            "remove": remove_dir
        })
        
        if not dry_run:
            if remove_dir:
                # Remove entire directory (agency folders in registry/store)
                shutil.rmtree(dir_path)
                stats["deleted"] += file_count if file_count else 1
            else:
                # Delete contents but keep the directory
                for item in dir_path.iterdir():
                    if item.name.startswith("."):
                        # Skip hidden files like .seq
                        continue
                    if item.is_file():
                        item.unlink()
                        stats["deleted"] += 1
                    elif item.is_dir():
                        shutil.rmtree(item)
                        stats["deleted"] += count_files(item)
    
    # Reset sequence counters
    if not dry_run:
        reset_sequences()
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Reset MDA POC execution artifacts")
    parser.add_argument("--force", "-f", action="store_true", help="Skip confirmation")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Show what would be deleted")
    
    args = parser.parse_args()
    
    print(f"\n{'=' * 60}")
    print("MDA POC RESET")
    print(f"{'=' * 60}\n")
    
    # Preview
    stats = reset(dry_run=True)
    
    if not stats["directories"]:
        print("✓ Nothing to reset - stores are already empty.")
        return
    
    print("The following will be cleared:\n")
    total_files = 0
    for d in stats["directories"]:
        print(f"  📁 {d['path']}: {d['files']} file(s)")
        total_files += d["files"]
    
    print(f"\n  Total: {total_files} file(s)")
    
    if args.dry_run:
        print("\n  (dry-run mode - no files deleted)")
        return
    
    if not args.force:
        response = input("\nProceed with reset? [y/N]: ")
        if response.lower() != "y":
            print("Cancelled.")
            sys.exit(0)
    
    # Actually reset
    stats = reset(dry_run=False)
    
    print(f"\n✅ Reset complete. Deleted {stats['deleted']} file(s).")


if __name__ == "__main__":
    main()
