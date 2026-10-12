"""Etapp B steg 5: hämtningsverktyget för jämförelsedata (SB-S5-c). Inget nätverk: en falsk opener lämnar
syntetiska SWEA-svar. Arkivrötter ligger i pytests tmp_path."""
import hashlib
import importlib.util
import io
import json
import os
import stat
import sys
import urllib.error
from datetime import UTC, datetime
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("fetch_benchmark", REPO / "scripts" / "data" / "fetch_benchmark.py")
fb = importlib.util.module_from_spec(spec)
sys.modules["fetch_benchmark"] = fb
spec.loader.exec_module(fb)

BODY = b'[{"date":"2020-01-02","value":9.3341},{"date":"2020-01-03","value":9.4068}]'
URL = fb.TARGETS[0].url


class Resp(io.BytesIO):
    def __init__(self, data, status=200):
        super().__init__(data)
        self.status = status


class Opener:
    def __init__(self, *answers):
        self.answers, self.urls = list(answers), []

    def __call__(self, req, timeout):
        self.urls.append(req.full_url)
        a = self.answers.pop(0)
        if isinstance(a, Exception):
            raise a
        return Resp(a)


def run(args, opener=None, clean=True, capsys=None):
    code = fb.main(args, opener=opener or Opener(), git_state=lambda: ("abc", clean),
                   clock=lambda: datetime(2026, 10, 12, tzinfo=UTC), sleep=lambda s: None)
    out = json.loads(capsys.readouterr().out) if capsys else None
    return code, out


@pytest.fixture
def root(tmp_path):
    r = tmp_path / "benchmark_archive"
    yield r
    for dp, dn, fn in os.walk(tmp_path):
        os.chmod(dp, 0o755)


def test_plan_and_dry_run_make_no_request(root, capsys):
    code, out = run(["plan", "--root", str(root)], capsys=capsys)
    assert code == 1 and out["missing"] == [fb.TARGETS[0].key]
    op = Opener()
    code, out = run(["fetch", "--root", str(root)], op, capsys=capsys)
    assert code == 1 and op.urls == [] and not root.exists()


def test_apply_requires_clean_worktree(root, capsys):
    op = Opener(BODY)
    code, _ = run(["fetch", "--root", str(root), "--apply"], op, clean=False)
    assert code == 5 and op.urls == [] and not root.exists()


def test_fetch_build_verify_and_idempotence(root, capsys):
    op = Opener(BODY)
    code, out = run(["fetch", "--root", str(root), "--apply"], op, capsys=capsys)
    assert code == 0 and op.urls == [URL] and out["requests"] == 1
    assert out["fetched"][0]["observations"] == 2 and "9.3341" not in json.dumps(out)     # inga värden i utdata
    obj = root / "objects" / fb.TARGETS[0].key / "response.json"
    assert obj.read_bytes() == BODY and stat.S_IMODE(obj.stat().st_mode) == 0o444
    assert stat.S_IMODE(obj.parent.stat().st_mode) == 0o555
    code, out = run(["verify", "--root", str(root)], capsys=capsys)
    assert code == 1 and [f["code"] for f in out["findings"]] == ["CANONICAL_MISSING"]
    code, out = run(["build", "--root", str(root), "--apply"], capsys=capsys)
    canon = root / "canonical" / "riksbank-SEKUSDPMI.csv"
    assert code == 0 and canon.read_bytes() == b"obs_date,value_scaled\n2020-01-02,9334100\n2020-01-03,9406800\n"
    assert out["built"][0]["sha256"] == hashlib.sha256(canon.read_bytes()).hexdigest()
    assert run(["verify", "--root", str(root)], capsys=capsys)[0] == 0
    op2 = Opener()
    code, out = run(["fetch", "--root", str(root), "--apply"], op2, capsys=capsys)
    assert code == 0 and op2.urls == [] and out["requests"] == 0
    assert len(os.listdir(root / "runs")) == 3


def test_invalid_response_is_not_stored(root, capsys):
    code, out = run(["fetch", "--root", str(root), "--apply"], Opener(b'{"error":"x"}'), capsys=capsys)
    assert code == 1 and out["failures"] and not (root / "objects" / fb.TARGETS[0].key).exists()


def test_recheck_keeps_original_and_stores_revision(root, capsys):
    run(["fetch", "--root", str(root), "--apply"], Opener(BODY))
    capsys.readouterr()
    assert run(["recheck", "--root", str(root)], Opener(BODY), capsys=capsys)[0] == 0
    changed = BODY.replace(b"9.4068", b"9.4069")
    code, out = run(["recheck", "--root", str(root), "--apply"], Opener(changed), capsys=capsys)
    h = hashlib.sha256(changed).hexdigest()
    assert code == 2 and out["changes"] == [{"key": fb.TARGETS[0].key, "sha256": h}]
    assert (root / "objects" / fb.TARGETS[0].key / "response.json").read_bytes() == BODY
    assert (root / "revisions" / fb.TARGETS[0].key / h / "response.json").read_bytes() == changed


def test_network_rules():
    with pytest.raises(fb.RefusedError):
        fb.check_url("https://www.ishares.com/x")                      # inga iShares-värdar (SB-S5-b)
    with pytest.raises(fb.RefusedError):
        fb.check_url("http://api.riksbank.se/swea/v1/x")
    assert fb.ALLOWED_HOSTS == frozenset({"api.riksbank.se"})
    op = Opener(urllib.error.URLError("x"), urllib.error.HTTPError(URL, 503, "", None, None), BODY)
    assert fb.download(URL, op, lambda s: None) == BODY and len(op.urls) == 3
    op = Opener(urllib.error.HTTPError(URL, 404, "", None, None), BODY)
    with pytest.raises(fb.FetchError):
        fb.download(URL, op, lambda s: None)
    assert len(op.urls) == 1
    op = Opener(urllib.error.HTTPError(URL, 302, "redirect refused", None, None), BODY)
    with pytest.raises(fb.FetchError):
        fb.download(URL, op, lambda s: None)
    assert len(op.urls) == 1
    with pytest.raises(fb.FetchError):
        fb.download(URL, Opener(b"x" * (fb.MAX_BYTES + 1)), lambda s: None)


def test_verify_detects_tampering_and_unknown_entries(root, capsys):
    run(["fetch", "--root", str(root), "--apply"], Opener(BODY))
    run(["build", "--root", str(root), "--apply"])
    capsys.readouterr()
    obj = root / "objects" / fb.TARGETS[0].key / "response.json"
    os.chmod(obj.parent, 0o755)
    os.chmod(obj, 0o644)
    obj.write_bytes(b"[]")
    (root / "extra").write_text("x")
    code, out = run(["verify", "--root", str(root)], capsys=capsys)
    got = {f["code"] for f in out["findings"]}
    assert code == 3 and {"OBJECT_INVALID", "UNKNOWN_ENTRY"} <= got


def test_refusals_and_usage(tmp_path, capsys):
    other = tmp_path / "other"
    other.mkdir()
    (other / "x").write_text("x")
    assert run(["fetch", "--root", str(other), "--apply"])[0] == 5
    assert run(["verify", "--root", str(tmp_path / "missing")])[0] == 5
    assert run(["plan", "--root", str(tmp_path / "a" / "b")])[0] == 5         # föräldern saknas
    assert run(["verify", "--root", str(tmp_path), "--apply"])[0] == 64
    assert run(["nope", "--root", str(tmp_path)])[0] == 64
    bad = tmp_path / "bad"
    bad.mkdir()
    (bad / "ARCHIVE.json").write_text("{}")
    assert run(["verify", "--root", str(bad)])[0] == 3
