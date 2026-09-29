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


def test_credentials_are_secrets():
    properties = TapGainsight.config_jsonschema["properties"]
    for name in ("access_key", "client_id", "client_secret"):
        assert properties[name].get("secret") is True
    # Either credential method may be set, so only domain is required.
    assert TapGainsight.config_jsonschema["required"] == ["domain"]
