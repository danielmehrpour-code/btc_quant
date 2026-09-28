from dataclasses import replace
from pathlib import Path

import pytest

from btc_trading_system.config.settings import (LiveTradingNotAllowed, Settings,
                                                TradingConfig, assert_live_allowed,
                                                load_settings)

CFG = Path(__file__).parent.parent / "configs" / "default.yaml"
GOOD_ENV = {"LIVE_TRADING": "true", "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_THE_RISK",
            "EXCHANGE_API_KEY": "k", "EXCHANGE_API_SECRET": "s"}


def test_default_live_is_false():
    assert Settings().trading.live is False
    assert load_settings(CFG).trading.live is False


def test_default_config_loads_expected_values():
    s = load_settings(CFG)
    assert s.market.symbol == "BTCUSDT" and s.primary_timeframe == "15m"
    assert s.risk.risk_per_trade == 0.005


def test_live_blocked_by_default():
    with pytest.raises(LiveTradingNotAllowed):
        assert_live_allowed(Settings(), env={})


def _live_settings():
    return replace(Settings(), trading=TradingConfig(live=True))


def test_live_needs_every_check():
    s = _live_settings()
    assert_live_allowed(s, GOOD_ENV, paper_validated=True)  # allt ok
    for missing in GOOD_ENV:
        env = {k: v for k, v in GOOD_ENV.items() if k != missing}
        with pytest.raises(LiveTradingNotAllowed):
            assert_live_allowed(s, env, paper_validated=True)
    with pytest.raises(LiveTradingNotAllowed):
        assert_live_allowed(s, GOOD_ENV, paper_validated=False)


def test_zero_latency_config_rejected():
    from btc_trading_system.config.settings import ExecutionConfig
    with pytest.raises(ValueError):
        Settings(execution=ExecutionConfig(min_latency_bars=0))


def test_excessive_risk_per_trade_rejected():
    from btc_trading_system.config.settings import RiskConfig
    with pytest.raises(ValueError):
        Settings(risk=RiskConfig(risk_per_trade=0.5))
