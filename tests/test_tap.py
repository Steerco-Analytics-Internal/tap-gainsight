"""Smoke tests for the tap class."""

from tap_gainsight.tap import TapGainsight
from tests.conftest import make_tap


def test_tap_instantiates(api):
    tap = make_tap()
    assert tap.name == "tap-gainsight"


def test_streams_discoverable(api):
    streams = make_tap().discover_streams()
    assert len(streams) >= 4
    assert all(s.name for s in streams)


def test_access_key_is_a_secret():
    schema = TapGainsight.config_jsonschema["properties"]["access_key"]
    assert schema.get("secret") is True
    assert TapGainsight.config_jsonschema["required"] == ["access_key", "domain"]
