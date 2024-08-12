from narwhals import Series


def to_lower(s: Series) -> Series:
    return s.str.to_lowercase()
