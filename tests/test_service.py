import pytest

from omnicall.service import normalize_phone_number


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("+49 (621) 123-45", "+4962112345"),
        ("0049 621 12345", "+4962112345"),
        ("0621/12345", "062112345"),
    ],
)
def test_normalize_phone_number(source: str, expected: str) -> None:
    assert normalize_phone_number(source) == expected


@pytest.mark.parametrize("source", ["", "12", "+49;Hangup()", "abc123"])
def test_normalize_phone_number_rejects_invalid_values(source: str) -> None:
    with pytest.raises(ValueError):
        normalize_phone_number(source)
