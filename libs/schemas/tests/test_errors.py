"""Problem Details contract and PAN masking."""

from schemas import FieldError, ProblemDetails, is_luhn, mask_pans
from schemas.errors import problem_body, type_for_http


def test_mask_pans_keeps_first6_last4() -> None:
    assert mask_pans("card 4111111111111111 ok") == "card 411111******1111 ok"


def test_problem_body_roundtrip() -> None:
    body = problem_body(
        type="validation_error",
        title="Request validation failed",
        status=422,
        request_id="req-1",
        errors=[FieldError(field="pan", message="bad")],
    )
    parsed = ProblemDetails.model_validate(body)
    assert parsed.type == "validation_error"
    assert parsed.errors[0].field == "pan"
    assert is_luhn("4111111111111111")


def test_type_for_http_maps_known_details() -> None:
    assert type_for_http(409, "invalid transition") == "invalid_transition"
    assert type_for_http(503, "features unavailable") == "upstream_unavailable"
    assert type_for_http(504) == "upstream_timeout"
