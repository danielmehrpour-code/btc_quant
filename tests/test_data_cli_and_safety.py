import contextlib
import io
import re
from pathlib import Path

from btc_trading_system.config.settings import Settings, load_settings
from btc_trading_system.data.cli import main
from data_helpers import RawBackend, make_klines, write_csv

ROOT = Path(__file__).parent.parent
PKG = ROOT / "btc_trading_system"


def _run(argv):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main(argv, backend=RawBackend())
    return code, out.getvalue(), err.getvalue()


def test_cli_success(tmp_path):
    f = write_csv(tmp_path / "a.csv", make_klines("1h", 40).drop(index=[10]).reset_index(drop=True))
    code, out, _ = _run([str(f), "--store", str(tmp_path / "st"), "--instrument", "perp", "--timeframe", "1h"])
    assert code == 0 and "binance/perp/BTCUSDT/1h" in out and "luckor=1" in out


def test_cli_reports_failure_with_nonzero_exit(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("garbage\n")
    code, _, err = _run([str(bad), "--store", str(tmp_path / "st"), "--instrument", "spot", "--timeframe", "15m"])
    assert code == 2 and "INGEST FAILED" in err
    assert not (tmp_path / "st").exists()


def test_cli_wrong_timeframe_fails(tmp_path):
    f = write_csv(tmp_path / "a.csv", make_klines("15m", 20))
    code, _, err = _run([str(f), "--store", str(tmp_path / "st"), "--instrument", "spot", "--timeframe", "5m"])
    assert code == 2 and "CLOSE_TIME_MISMATCH" in err


# ---- säkerhet: inget nätverk, ingen live trading ----
FORBIDDEN = re.compile(r"^\s*(import|from)\s+(socket|requests|urllib|http|httpx|aiohttp|websocket|websockets|ftplib|ccxt|binance)\b", re.M)


def test_data_layer_has_no_network_imports():
    files = list((PKG / "data").glob("*.py")) + list((PKG / "data_validation").glob("*.py"))
    assert len(files) >= 8
    for f in files:
        assert not FORBIDDEN.search(f.read_text()), f"nätverksimport i {f.name}"


def test_data_layer_does_not_touch_brokers_or_live_flags():
    for f in list((PKG / "data").glob("*.py")) + list((PKG / "data_validation").glob("*.py")):
        txt = f.read_text()
        assert "brokers" not in txt and "LIVE_TRADING" not in txt and "API_KEY" not in txt, f.name


def test_live_trading_still_disabled_by_default():
    assert Settings().trading.live is False
    assert load_settings(ROOT / "configs" / "default.yaml").trading.live is False
