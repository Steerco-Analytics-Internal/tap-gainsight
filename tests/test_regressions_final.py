"""Regression tests for the final review's two changes.

Each test failed on commit 65d3ac4, before its fix.
"""

from __future__ import annotations

import datetime
import json
import logging

import pytest
from singer_sdk.exceptions import ConfigValidationError, FatalAPIError

from tests.conftest import BASE_URL, QueryEngine, make_tap, query_url
from tests.test_regressions_delta import cta_rows, iso_z

UTC = datetime.timezone.utc
T0 = 1707121475253  # 2024-02-05T08:24:35.253Z, in winter: Los Angeles is UTC-8.
CTA_URL = f"{BASE_URL}/v2/cockpit/cta/list"


def records_of(capsys):
    out = capsys.readouterr().out.splitlines()
    return [json.loads(line)["record"] for line in out if '"RECORD"' in line]


# 1. An overflowing shortest CTA window is read twice and merged by Gsid.


@pytest.mark.parametrize("count, page_size", [(5, 2), (6, 4)])
def test_an_overflowing_shortest_window_is_read_twice_and_merged(api, capsys, caplog, count, page_size):
    logging.getLogger("tap-gainsight").addHandler(caplog.handler)
    day = (datetime.datetime.now(UTC) - datetime.timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = cta_rows([day + datetime.timedelta(hours=1, minutes=m) for m in range(count)])
    # The fake shuffles every response, so each pageNumber read of the
    # window skips some CTAs. With these sizes, one read misses rows.
    engine = api.serve(CTA_URL, QueryEngine(rows, {"ModifiedDate"}, unordered=True))
    stream = make_tap(start_date=iso_z(day)).streams["cta"]
    stream.page_size = page_size
    stream.sync()
    got = [r["Gsid"] for r in records_of(capsys)]
    assert sorted(got) == sorted(r["Gsid"] for r in rows)
    assert len(got) == len(set(got))
    window = [b for b in engine.bodies if b["where"]["conditions"][0]["operator"] == "BTW"]
    shortest = [day.date().isoformat(), (day.date() + datetime.timedelta(days=1)).isoformat()]
    first_pages = [b for b in window if b["pageNumber"] == 1 and b["where"]["conditions"][0]["value"] == shortest]
    assert len(first_pages) == 2
    assert "picked up only when that CTA is edited again" in caplog.text


# 2. The filter_timezone setting.


def test_format_query_datetime_follows_the_zone_rules_including_dst():
    from tap_gainsight.client import format_query_datetime, load_zone

    zone = load_zone("America/Los_Angeles")
    winter = datetime.datetime(2024, 2, 5, 12, 0, tzinfo=UTC)
    summer = datetime.datetime(2024, 7, 1, 12, 0, tzinfo=UTC)
    assert format_query_datetime(winter, zone) == "2024-02-05 04:00:00"
    assert format_query_datetime(summer, zone) == "2024-07-01 05:00:00"
    assert format_query_datetime(winter) == "2024-02-05 12:00:00"
    assert load_zone(None) is None and load_zone("") is None


def test_an_unknown_zone_is_a_config_error():
    with pytest.raises(ConfigValidationError, match="Unknown filter_timezone 'Mars/Olympus'"):
        make_tap(filter_timezone="Mars/Olympus")


def test_the_first_filter_is_sent_in_the_zone(api):
    engine = api.serve(query_url("Company"), QueryEngine([], {"ModifiedDate"}))
    make_tap(start_date="2024-01-02T00:00:00Z", filter_timezone="America/Los_Angeles").streams["Company"].sync()
    assert engine.bodies[0]["where"]["conditions"][0]["value"] == ["2023-12-31 16:00:00"]


def local_time_rows():
    return [{"Gsid": f"1P02C{i:04d}", "Name": f"Company {i}", "ModifiedDate": T0 + i * 1000} for i in range(5)]


def test_a_utc_minus_8_tenant_fails_loudly_without_the_setting(api):
    api.serve(query_url("Company"), QueryEngine(local_time_rows(), {"ModifiedDate"}, naive_offset_hours=-8))
    stream = make_tap().streams["Company"]
    stream.page_size = 2
    with pytest.raises(FatalAPIError, match="reads filter times in its local time zone, set filter_timezone"):
        stream.sync()


@pytest.mark.parametrize("name", ["Company", "deleted_records"])
def test_a_utc_minus_8_tenant_syncs_with_the_setting(api, capsys, name):
    if name == "Company":
        rows = local_time_rows()
        api.serve(query_url("Company"), QueryEngine(rows, {"ModifiedDate"}, naive_offset_hours=-8))
        key = "Gsid"
    else:
        rows = [
            {"RecordId": f"1P02D{i:04d}", "DeletedOn": f"2024-02-05T07:17:1{i}Z", "ObjectName": "Company"}
            for i in range(5)
        ]
        api.serve(query_url("record_delete_log"), QueryEngine(rows, {"DeletedOn"}, shape="records", naive_offset_hours=-8))
        api.serve(query_url("record_delete_log_high_volume"), QueryEngine([], {"DeletedOn"}, shape="records"))
        key = "RecordId"
    stream = make_tap(filter_timezone="America/Los_Angeles").streams[name]
    stream.page_size = 2
    stream.sync()
    got = [r[key] for r in records_of(capsys)]
    assert sorted(got) == sorted(r[key] for r in rows)
    assert len(got) == len(set(got))


def test_the_merge_keeps_ctas_without_a_gsid(api, capsys):
    day = (datetime.datetime.now(UTC) - datetime.timedelta(days=3)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = cta_rows([day + datetime.timedelta(hours=1, minutes=m) for m in range(3)])
    del rows[0]["Gsid"]
    api.serve(CTA_URL, QueryEngine(rows, {"ModifiedDate"}))
    stream = make_tap(start_date=iso_z(day)).streams["cta"]
    stream.page_size = 2
    stream.sync()
    got = records_of(capsys)
    assert sum(1 for r in got if "Gsid" not in r) == 2  # one per read: no key to merge on
    assert {r["Gsid"] for r in got if "Gsid" in r} == {r["Gsid"] for r in rows[1:]}


def test_an_unknown_zone_is_reported_when_errors_are_not_raised(api):
    tap = make_tap()
    tap._config = {**tap.config, "filter_timezone": "Mars/Olympus"}
    _, errors = tap._validate_config(raise_errors=False)
    assert any("Unknown filter_timezone" in error for error in errors)
