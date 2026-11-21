"""PAN checksum and masking helpers."""

from __future__ import annotations

import re

_PAN_RUN = re.compile(r"(?<!\d)\d{13,19}(?!\d)")


def is_luhn(pan: str) -> bool:
    """Return True if ``pan`` passes the Luhn checksum."""
    total = 0
    for index, char in enumerate(reversed(pan)):
        digit = ord(char) - 48
        if index % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def mask_pans(text: str) -> str:
    """Mask maximal 13–19 digit runs, keeping the first 6 and last 4 digits."""

    def replace(match: re.Match[str]) -> str:
        digits = match.group(0)
        hidden = len(digits) - 10
        return f"{digits[:6]}{'*' * hidden}{digits[-4:]}"

    return _PAN_RUN.sub(replace, text)
