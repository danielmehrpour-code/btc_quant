import importlib
import inspect

import pytest

from btc_trading_system.core import interfaces

NAMES = ["DataProvider", "FeatureEngine", "Strategy", "RiskEngine", "Portfolio",
         "OrderManager", "ExecutionEngine", "Broker", "Backtester",
         "PerformanceAnalyzer", "Monitor"]


def test_all_required_interfaces_exist_and_are_abstract():
    for n in NAMES:
        cls = getattr(interfaces, n)
        assert inspect.isabstract(cls), n
        with pytest.raises(TypeError):
            cls()


def test_all_spec_packages_importable():
    for p in "config data data_validation features indicators strategies regimes models "\
             "backtesting execution brokers portfolio risk database monitoring dashboard "\
             "experiments".split():
        importlib.import_module(f"btc_trading_system.{p}")


def test_minimal_feature_engine_is_swappable():
    import pandas as pd

    class Dummy(interfaces.FeatureEngine):
        lookback = 1
        def compute(self, bars):
            return bars[["close"]].pct_change()

    out = Dummy().compute(pd.DataFrame({"close": [1.0, 2.0]}))
    assert len(out) == 2
