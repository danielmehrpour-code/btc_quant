"""Phase 3 CLI: returkoder, read-only mot lagret, inga nätverksimporter."""
import contextlib
import hashlib
import io
import json
import re
from pathlib import Path

from btc_trading_system.data import quality_cli

PKG = Path(__file__).resolve().parents[2] / "btc_trading_system"
NEW_MODULES = (
    PKG / "data_validation" / "quality_config.py",
    PKG / "data_validation" / "invariants.py",
    PKG / "data_validation" / "anomalies.py",
    PKG / "data_validation" / "consistency.py",
    PKG / "data_validation" / "quality_report.py",
    PKG / "data" / "quality_cli.py",
)
NETWORK = re.compile(
    r"^\s*(import|from)\s+(socket|requests|urllib|http|httpx|aiohttp|websockets?|ftplib|ccxt|"
    r"binance)\b",
    re.MULTILINE,
)


def _tree_digest(root: Path) -> dict[str, str]:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def _run(argv, qh):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = quality_cli.main(argv, backend=qh.TestBackend(), git=lambda: ("abc", True))
    return code, out.getvalue(), err.getvalue()


def _args(cmd, store, out, *extra):
    return [cmd, "--store", str(store), "--out", str(out), *extra]


def test_dataset_pass_and_store_untouched(qh, tmp_path):
    qh.put_dataset(tmp_path / "store", qh.make_bars("15m", 800), "spot", "15m")
    before = _tree_digest(tmp_path / "store")
    code, out, _ = _run(_args("dataset", tmp_path / "store", tmp_path / "out", "--instrument",
                              "spot", "--timeframe", "15m"), qh)
    assert code == 0 and out.startswith("PASS ")
    assert _tree_digest(tmp_path / "store") == before                    # read-only
    files = sorted(p.name for p in (tmp_path / "out").iterdir())
    assert len(files) == 2 and all(".quality." in f for f in files)


def test_exit_codes_fail_incomplete_error(qh, tmp_path):
    bad = qh.make_bars("15m", 800)
    bad.loc[bad.index[50], "trade_count"] = 0
    qh.put_dataset(tmp_path / "a", bad, "spot", "15m")
    code, _, _ = _run(_args("dataset", tmp_path / "a", tmp_path / "o1", "--instrument", "spot",
                            "--timeframe", "15m"), qh)
    assert code == 1
    qh.put_dataset(tmp_path / "b", qh.make_bars("15m", 100), "spot", "15m")
    code, out, _ = _run(_args("dataset", tmp_path / "b", tmp_path / "o2", "--instrument", "spot",
                              "--timeframe", "15m"), qh)
    assert code == 3 and out.startswith("INCOMPLETE")
    code, _, err = _run(_args("dataset", tmp_path / "b", tmp_path / "o3", "--instrument", "spot",
                              "--timeframe", "1h"), qh)
    assert code == 2 and "QUALITY FAILED" in err


def test_out_inside_store_refused(qh, tmp_path):
    qh.put_dataset(tmp_path / "s", qh.make_bars("15m", 300), "spot", "15m")
    before = _tree_digest(tmp_path / "s")
    for out in (tmp_path / "s", tmp_path / "s" / "reports"):
        code, _, err = _run(_args("dataset", tmp_path / "s", out, "--instrument", "spot",
                                  "--timeframe", "15m"), qh)
        assert code == 2 and "datalagret" in err
    assert _tree_digest(tmp_path / "s") == before


def test_tf_and_xinst_commands(qh, tmp_path):
    lower = qh.make_bars("1m", 15 * 40)
    qh.put_dataset(tmp_path / "s", lower, "spot", "1m")
    qh.put_dataset(tmp_path / "s", qh.aggregate(lower, "1m", "15m"), "spot", "15m")
    code, out, _ = _run(_args("tf", tmp_path / "s", tmp_path / "o", "--instrument", "spot",
                              "--lower", "1m", "--higher", "15m"), qh)
    assert code == 0 and "tf_spot_BTCUSDT_1m-15m" in out
    bars = qh.make_bars("15m", 800)
    qh.put_dataset(tmp_path / "x", bars, "spot", "15m")
    qh.put_dataset(tmp_path / "x", bars, "perp", "15m")
    code, out, _ = _run(_args("xinst", tmp_path / "x", tmp_path / "o", "--timeframe", "15m"), qh)
    assert code == 0 and out.startswith("PASS ") and "xinst_BTCUSDT_15m" in out


def test_phase2_suspect_exit_code_4(qh, tmp_path):
    store, key, _ = qh.put_dataset(tmp_path / "s", qh.make_bars("15m", 300), "spot", "15m")
    mpath = store.manifest_path(key)
    manifest = json.loads(mpath.read_text())
    manifest["missing_bars_total"] = 5
    mpath.write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    code, _, err = _run(_args("dataset", tmp_path / "s", tmp_path / "o", "--instrument", "spot",
                              "--timeframe", "15m"), qh)
    assert code == 4 and "PHASE 2 SUSPECT" in err


def test_git_state_outside_repo(tmp_path):
    assert quality_cli.git_state(tmp_path) == ("unknown", None)


def test_new_modules_have_no_network_imports_and_no_phase2_writes():
    for path in NEW_MODULES:
        text = path.read_text(encoding="utf-8")
        assert not NETWORK.search(text), path.name
        assert "LIVE_TRADING" not in text and "API_KEY" not in text, path.name
        for forbidden in ("store.write", "backend.write", ".write_db", "DatasetStore.write"):
            assert forbidden not in text, (path.name, forbidden)
