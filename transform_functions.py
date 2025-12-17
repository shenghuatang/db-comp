"""
Transformation functions for join columns
These functions can be applied to columns before joining/comparison
"""

import pandas as pd
import re
from typing import Any


def remove_prefix_and_int(value: Any) -> int:
    """
    Remove 'ext-' prefix and convert to integer
    Example: 'ext-1001' -> 1001

    Args:
        value: Input value (can be string, int, or other)

    Returns:
        Integer value after removing prefix
    """
    if pd.isna(value):
        return None

    # Convert to string first
    str_value = str(value)

    # Remove 'ext-' prefix (case insensitive)
    cleaned = re.sub(r'^ext-?', '', str_value, flags=re.IGNORECASE)

    # Convert to integer
    try:
        return int(cleaned)
    except ValueError:
        # If conversion fails, try to extract digits
        digits = re.findall(r'\d+', cleaned)
        if digits:
            return int(digits[0])
        return None


def remove_prefix(value: Any, prefix: str = 'ext-') -> str:
    """
    Remove a specific prefix from value

    Args:
        value: Input value
        prefix: Prefix to remove (default: 'ext-')

    Returns:
        String value after removing prefix
    """
    if pd.isna(value):
        return None

    str_value = str(value)
    if str_value.startswith(prefix):
        return str_value[len(prefix):]
    return str_value


def to_int(value: Any) -> int:
    """
    Convert value to integer

    Args:
        value: Input value

    Returns:
        Integer value
    """
    if pd.isna(value):
        return None

    try:
        return int(value)
    except (ValueError, TypeError):
        # Try to extract digits
        digits = re.findall(r'\d+', str(value))
        if digits:
            return int(digits[0])
        return None


def to_str(value: Any) -> str:
    """
    Convert value to string

    Args:
        value: Input value

    Returns:
        String value
    """
    if pd.isna(value):
        return None
    return str(value)


def lowercase(value: Any) -> str:
    """
    Convert value to lowercase

    Args:
        value: Input value

    Returns:
        Lowercase string value
    """
    if pd.isna(value):
        return None
    return str(value).lower()


def uppercase(value: Any) -> str:
    """
    Convert value to uppercase

    Args:
        value: Input value

    Returns:
        Uppercase string value
    """
    if pd.isna(value):
        return None
    return str(value).upper()


def strip_whitespace(value: Any) -> str:
    """
    Remove leading and trailing whitespace

    Args:
        value: Input value

    Returns:
        Trimmed string value
    """
    if pd.isna(value):
        return None
    return str(value).strip()


def extract_digits(value: Any) -> str:
    """
    Extract only digits from value

    Args:
        value: Input value

    Returns:
        String containing only digits
    """
    if pd.isna(value):
        return None

    digits = re.findall(r'\d+', str(value))
    return ''.join(digits) if digits else None


# Registry of available transformation functions
TRANSFORM_FUNCTIONS = {
    'remove_prefix_and_int': remove_prefix_and_int,
    'remove_prefix': remove_prefix,
    'to_int': to_int,
    'to_str': to_str,
    'lowercase': lowercase,
    'uppercase': uppercase,
    'strip_whitespace': strip_whitespace,
    'extract_digits': extract_digits,
}


def apply_transform(series: pd.Series, function_name: str) -> pd.Series:
    """
    Apply a transformation function to a pandas Series

    Args:
        series: Pandas Series to transform
        function_name: Name of the transformation function

    Returns:
        Transformed Series
    """
    if function_name not in TRANSFORM_FUNCTIONS:
        raise ValueError(f"Unknown transformation function: {function_name}. "
                        f"Available functions: {list(TRANSFORM_FUNCTIONS.keys())}")

    transform_func = TRANSFORM_FUNCTIONS[function_name]
    return series.apply(transform_func)


def get_available_functions():
    """
    Get list of available transformation functions

    Returns:
        List of function names
    """
    return list(TRANSFORM_FUNCTIONS.keys())

