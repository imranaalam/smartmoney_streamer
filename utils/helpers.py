# utils/helpers.py

import pandas as pd
import logging

def format_date(date_input, input_format="%d %b %Y", output_format="%Y-%m-%d"):
    """
    Converts a date string from one format to another.

    Args:
        date_input (str or datetime-like): The date to format.
        input_format (str): The current format of the date string (if string).
        output_format (str): The desired format of the date string.

    Returns:
        str: The formatted date string, or None if formatting fails.
    """
    try:
        if isinstance(date_input, str):
            date = pd.to_datetime(date_input, format=input_format)
        else:
            date = pd.to_datetime(date_input)
        return date.strftime(output_format)
    except Exception as e:
        logging.error(f"Date formatting failed for '{date_input}': {e}")
        return None
