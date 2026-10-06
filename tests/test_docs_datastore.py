"""PHASE 4 steg 10: driftkontroller för docs/PHASE4_DATASTORE.md (P4-S9-f (2) och (3)).

Tabellerna i dokumentet som speglar kod jämförs med koden: CLI-kommandon, argument och exitkoder
(avsnitt 8) samt fyndkoder och allvarlighet (avsnitt 7). Radreferenser (`fil:rad`) kontrolleras inte
här; de förnyas i steg 13 (P4-S9-d).
"""
import argparse
import re
from pathlib import Path

from btc_trading_system.database import cli
from btc_trading_system.database import integrity as ig

ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "PHASE4_DATASTORE.md"
_CELL_SPLIT = re.compile(r"(?<!\\)\|")


def section(number: int) -> str:
    text = DOC.read_text(encoding="utf-8")
    start = text.index(f"\n## {number}. ")
    end = text.find("\n## ", start + 1)
    return text[start:end if end != -1 else len(text)]


def table(text: str, header: str) -> list[list[str]]:
    """Raderna i Markdown-tabellen vars rubrikrad börjar med `header` (utan rubrik och avdelare)."""
    lines = text.splitlines()
    i = next(n for n, line in enumerate(lines) if line.startswith(header))
    rows = []
    for line in lines[i + 2:]:
        if not line.startswith("|"):
            break
        rows.append([c.strip().replace("\\|", "|") for c in _CELL_SPLIT.split(line.strip())[1:-1]])
    return rows


def parser_options() -> dict[str, set[str]]:
    """Kommando -> argument (första flaggnamnet; positionella med dest) enligt cli._parser()."""
    out: dict[str, set[str]] = {}

    def walk(p: argparse.ArgumentParser, name: str) -> None:
        opts = set()
        for a in p._actions:
            if isinstance(a, argparse._SubParsersAction):
                for sub, sp in a.choices.items():
                    walk(sp, f"{name} {sub}".strip())
            elif not isinstance(a, argparse._HelpAction):
                opts.add(a.option_strings[0] if a.option_strings else a.dest)
        out[name] = opts
    walk(cli._parser(), "")
    return {k: v for k, v in out.items() if k in cli.HANDLERS}


def test_command_table_equals_the_parser():
    rows = table(section(8), "| Command | Arguments | Mutating |")
    doc = {}
    for command, args, mutating in rows:
        name = command.strip("`")
        opts = set(re.findall(r"--[a-z][a-z-]*", args)) | ({"files"} if "FILE..." in args else set())
        doc[name] = (opts, mutating)
    assert set(doc) == set(cli.HANDLERS)
    parsed = parser_options()
    for name, (opts, mutating) in doc.items():
        assert opts == parsed[name], name
        assert mutating == ("yes" if name in cli.MUTATING else "no"), name


def test_exit_code_table_equals_the_cli_constants():
    rows = table(section(8), "| Code | Meaning |")
    doc = {int(c) for row in rows for c in re.findall(r"\d+", row[0])}
    code = {cli.EXIT_OK, cli.EXIT_NOT_OK, cli.EXIT_REFUSED, cli.EXIT_STORE_ERROR, cli.EXIT_USAGE,
            cli.EXIT_INTERNAL} | set(ig.SEVERITY_EXIT.values())
    assert doc == code


def test_severity_table_equals_integrity():
    rows = table(section(7), "| Severity | Exit | Repaired? |")
    assert {sev: int(exit_) for sev, exit_, _ in rows} == ig.SEVERITY_EXIT


def test_finding_code_table_equals_integrity_codes():
    rows = table(section(7), "| Area | Codes |")
    doc = {}
    for _, codes in rows:
        for item in codes.split(", "):
            m = re.fullmatch(r"([A-Z][A-Z_]+)(?: \((ERROR|PENDING|REPAIRABLE|WARN|INFO)\))?(?: \([^)]*\))?", item)
            assert m, item
            doc[m.group(1)] = m.group(2) or "ERROR"
    assert doc == ig.CODES
