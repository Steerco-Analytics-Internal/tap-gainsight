"""The fixtures are doc samples. These tests keep them honest."""

import json
import re

from tests.conftest import FIXTURES

DOCS_PREFIX = "https://support.gainsight.com/gainsight_nxt/API_and_Developer_Docs/"


def manifest():
    return json.loads((FIXTURES / "SOURCES.json").read_text())


def test_every_fixture_names_its_doc_source():
    files = {p.name for p in FIXTURES.iterdir() if p.name != "SOURCES.json"}
    entries = manifest()
    assert files == set(entries)
    for name, entry in entries.items():
        assert entry["source"].startswith(DOCS_PREFIX), name
        assert entry["section"], name


def test_describe_repair_adds_only_the_missing_comma():
    verbatim = (FIXTURES / "describe_response.verbatim.txt").read_text()
    repaired = (FIXTURES / "describe_response.json").read_text()
    try:
        json.loads(verbatim)
        raise AssertionError("The verbatim doc sample is expected to be invalid JSON.")
    except json.JSONDecodeError:
        pass
    assert repaired.replace('"sourceType": "STRING",\n', '"sourceType": "STRING"\n', 1) == verbatim
    json.loads(repaired)


def test_cta_request_repair_removes_only_comments():
    verbatim = (FIXTURES / "cta_list_request.verbatim.txt").read_text()
    repaired = json.loads((FIXTURES / "cta_list_request.json").read_text())
    assert json.loads(re.sub(r"\s*//[^\n]*", "", verbatim)) == repaired
