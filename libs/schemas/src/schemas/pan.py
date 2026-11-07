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
