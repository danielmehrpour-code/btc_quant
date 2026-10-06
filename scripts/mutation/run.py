"""Ingång för Phase 4:s mutationstester: python scripts/mutation/run.py --scratch DIR ... (se runner.py)."""
import importlib.util
import sys
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location("mutation_runner", Path(__file__).resolve().parent / "runner.py")
assert _SPEC is not None and _SPEC.loader is not None
runner = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(runner)

if __name__ == "__main__":
    sys.exit(runner.main())
