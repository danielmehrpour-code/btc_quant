# Environment

## Supported (verified)
Linux or WSL2, Ubuntu 24.04, Python 3.12.x in a venv. See VERIFICATION.md for exact versions.

```bash
python3 -m venv .venv && source .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
python -m pytest -v
```
Keep the repo inside the Linux file system (`~/...`), not under `/mnt/c/...`.

## Native Windows – not supported (known issue)
On Windows 10.0.26200 with Python 3.14.7, `import pandas` failed with
"An Application Control policy has blocked this file" (Swedish: "En programkontrollprincip har
blockerat den här filen"). This comes from Windows application control (Smart App Control
or an App Control for Business policy) blocking pandas' compiled extension files. It is not a project bug.
Do not disable Windows security to work around it; use WSL2.

## Reproducibility
After a passing run, record the exact environment:
`python -m pip freeze --exclude-editable > requirements-lock.txt`

## Addition (recorded 2026-09-30)
This file was drafted during V1 (2026-09-28) but never committed; VERIFICATION.md already
referred to it. The sections above are the original draft. This section is new:

- Phase 4 (database) requires SQLite >= 3.37 (STRICT tables). Verified in WSL2 with
  SQLite 3.45.1.
- Which Windows policy caused the block (Smart App Control or App Control for Business) was
  not determined; only the error message above is confirmed.
