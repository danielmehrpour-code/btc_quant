"""Kontrollerar varje kodreferens (`fil:rad`, `fil:rad-rad`, med fortsättningar `:rad` i samma cell) och
varje besluts-ID i ett dokument mot repot (P4-S9-e, P4-S13-b).

Användning:  python scripts/docs/check_refs.py <dokument.md> [--show]

Kodreferenser läses mot den incheckade versionen (`git show HEAD:<fil>`); ett filnamn utan katalog avser
btc_trading_system/database/. Besluts-ID:n slås upp i arbetskatalogens docs/DECISIONS.md. --show skriver
första raden i varje citerat intervall för granskning. Exit 0 endast om inga problem hittades.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DB = "btc_trading_system/database/"
# `file.py:12`, `file.py:12-20`, följt av valfria fortsättningar `, `:34`` i samma cell
_REF = re.compile(r"`((?:[\w./-]+/)?[\w.-]+\.(?:py|md|yaml)):(\d+)(?:-(\d+))?`((?:,\s*`:\d+(?:-\d+)?`)*)")
_CONT = re.compile(r"`:(\d+)(?:-(\d+))?`")
_ID = re.compile(r"\b(G-\d\d|P\d-\d\d|P\d-S\d+[a-z]?-[A-Za-z0-9-]+|V3-A|U-\d\d)\b")
_ROW_ID = re.compile(r"^\| ([A-Z][A-Za-z0-9-]+) \|", re.MULTILINE)


def resolve(name: str) -> str:
    return name if "/" in name else DB + name


def code_refs(doc: str) -> list[tuple[str, int, int]]:
    """(sökväg, första rad, sista rad) för varje citerat intervall, i dokumentets ordning."""
    out = []
    for m in _REF.finditer(doc):
        path = resolve(m.group(1))
        out.append((path, int(m.group(2)), int(m.group(3) or m.group(2))))
        out += [(path, int(a), int(b or a)) for a, b in _CONT.findall(m.group(4))]
    return out


def decision_ids(doc: str) -> set[str]:
    return set(_ID.findall(doc))


def known_decisions(decisions_md: str) -> set[str]:
    return set(_ROW_ID.findall(decisions_md))


def head_lines(path: str, repo: Path = ROOT) -> list[str] | None:
    r = subprocess.run(["git", "-C", str(repo), "show", f"HEAD:{path}"], capture_output=True, text=True,
                       check=False)
    return r.stdout.splitlines() if r.returncode == 0 else None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Kontrollera kodreferenser och besluts-ID:n i ett dokument.",
                                 allow_abbrev=False)
    ap.add_argument("doc", type=Path)
    ap.add_argument("--show", action="store_true")
    args = ap.parse_args(argv)
    doc = args.doc.read_text(encoding="utf-8")
    head = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"], capture_output=True,
                          text=True, check=True).stdout.strip()
    bad = 0
    refs = code_refs(doc)
    cache: dict[str, list[str] | None] = {}
    for path, a, b in refs:
        if path not in cache:
            cache[path] = head_lines(path)
        text = cache[path]
        if text is None:
            print(f"MISSING FILE {path}")
            bad += 1
        elif not (1 <= a <= b <= len(text)):
            print(f"OUT OF RANGE {path}:{a}-{b} (len {len(text)})")
            bad += 1
        elif args.show:
            print(f"{path}:{a}{'-' + str(b) if b != a else ''}: {text[a - 1].strip()[:110]}")
    ids = decision_ids(doc)
    missing = sorted(ids - known_decisions((ROOT / "docs" / "DECISIONS.md").read_text(encoding="utf-8")))
    for i in missing:
        print(f"UNKNOWN DECISION ID {i}")
    print(f"HEAD {head}: {len(refs)} code references, {len(ids)} decision IDs; problems: {bad + len(missing)}")
    return 1 if bad or missing else 0


if __name__ == "__main__":
    sys.exit(main())
