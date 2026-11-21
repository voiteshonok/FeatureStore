from uuid import uuid4

from features.windows import card_key, format_geo, geo_key, member, merchant_features_key
from pan_scan.scan import (
    exit_code,
    find_pans,
    format_report,
    redis_scan_texts,
    scan_records,
)

PAN = "4111111111111111"
MASKED = "411111******1111"
TIMESTAMP = "1700000000000"
# 13-digit Luhn-valid score lookalike (epoch-like); must not be scanned on card: scores.
LUHN_SCORE = "1234567890128"


def test_planted_pan_in_log_and_postgres_row() -> None:
    log_findings = scan_records("logs", [("logs ingestion line 4", f"saw {PAN} in a log")])
    row_findings = scan_records(
        "postgres",
        [("postgres public.notes row 0", f"note {PAN}")],
    )
    findings = log_findings + row_findings
    assert len(findings) == 2
    assert {item.source for item in findings} == {"logs", "postgres"}
    assert {item.masked for item in findings} == {MASKED}
    assert all(PAN not in item.masked for item in findings)
    assert exit_code(findings) == 1


def test_hex_token_and_decimal_fraction_are_not_pans() -> None:
    assert find_pans(f"ab{PAN}cd") == []
    assert find_pans(f'{{"lon":13.{PAN}}}') == []
    assert find_pans(f"saw {PAN} in a log") == [PAN]
    assert find_pans(f'{{"pan":"{PAN}"}}') == [PAN]


def test_millisecond_timestamp_is_not_a_pan() -> None:
    assert find_pans(f"event at {TIMESTAMP}") == []
    assert scan_records("logs", [("logs kafka line 1", f"ts {TIMESTAMP}")]) == []
    assert exit_code([]) == 0


def test_card_skips_score_but_scans_member() -> None:
    key = card_key("tok")
    event_id = uuid4()
    clean = redis_scan_texts(
        key,
        "zset",
        zset_members=[(member(event_id, 100), float(LUHN_SCORE))],
    )
    assert scan_records("redis", [("card-clean", " ".join(clean))]) == []

    planted = redis_scan_texts(
        key,
        "zset",
        zset_members=[(f"{event_id}|{PAN}", 1_700_000_000_000.0)],
    )
    findings = scan_records("redis", [("card-pan", " ".join(planted))])
    assert len(findings) == 1
    assert findings[0].masked == MASKED
    assert PAN not in findings[0].masked


def test_mfeat_scans_field_names_skips_values() -> None:
    key = merchant_features_key("m-1")
    value_only = redis_scan_texts(
        key,
        "hash",
        hash_items={"m:1:c": PAN},
    )
    assert scan_records("redis", [("mfeat-value", " ".join(value_only))]) == []

    field_planted = redis_scan_texts(
        key,
        "hash",
        hash_items={f"m:{PAN}:c": "1"},
    )
    findings = scan_records("redis", [("mfeat-field", " ".join(field_planted))])
    assert len(findings) == 1
    assert findings[0].masked == MASKED


def test_geo_skips_time_scans_coords_and_extra_field() -> None:
    key = geo_key("tok")
    clean = redis_scan_texts(
        key,
        "string",
        string_value=format_geo(52.5, 13.4, int(LUHN_SCORE)),
    )
    assert LUHN_SCORE not in " ".join(clean)
    assert scan_records("redis", [("geo-clean", " ".join(clean))]) == []

    in_lat = redis_scan_texts(
        key,
        "string",
        string_value=f"{PAN}|13.4|1700000000000",
    )
    assert scan_records("redis", [("geo-lat", " ".join(in_lat))])[0].masked == MASKED

    extra = redis_scan_texts(
        key,
        "string",
        string_value=f"52.5|13.4|1700000000000|{PAN}",
    )
    assert scan_records("redis", [("geo-extra", " ".join(extra))])[0].masked == MASKED


def test_unknown_prefix_scans_entire_zset_and_hash() -> None:
    zset_texts = redis_scan_texts(
        "other:key",
        "zset",
        zset_members=[("member-ok", float(PAN))],
    )
    assert PAN in " ".join(zset_texts)
    assert scan_records("redis", [("unk-zset", " ".join(zset_texts))])[0].masked == MASKED

    hash_texts = redis_scan_texts(
        "shadow:1",
        "hash",
        hash_items={"count": PAN},
    )
    assert scan_records("redis", [("unk-hash", " ".join(hash_texts))])[0].masked == MASKED


def test_format_report_shows_scanned_counts_not_findings() -> None:
    report = format_report([("redis", 12), ("clickhouse", 3)], [])
    assert "redis: 12" in report
    assert "clickhouse: 3" in report
    assert "total: 15" in report
    assert MASKED not in report
