"""Smoke tests. Expand once you've customized the streams."""

from tap_gainsight.tap import TapGainsight


def test_tap_instantiates():
    tap = TapGainsight(config={"api_key": "test"}, parse_env_config=False)
    assert tap.name == "tap-gainsight"


def test_streams_discoverable():
    tap = TapGainsight(config={"api_key": "test"}, parse_env_config=False)
    streams = tap.discover_streams()
    assert len(streams) >= 1
    assert all(s.name for s in streams)
