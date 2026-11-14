from pan_scan.scan import exit_code, find_pans, scan_records

PAN = "4111111111111111"
MASKED = "411111******1111"
TIMESTAMP = "1700000000000"


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
