import re
from datetime import datetime
from typing import Iterable, List


def match_names(string: str, names: Iterable[str]) -> List[str]:
    return [matched for name in names if (matched := match_name(string, name)) is not None]

def match_name(string: str, name: str) -> str:
    """
    Matches a string against a given name across UpperCamelCase, lowerCamelCase, and UnderscoreCase.

    Args:
        string (str): The string to match.
        name (str): The name to compare with, provided in any of the three formats.

    Returns:
        bool: True if the string matches the name in any format, False otherwise.
    """
    # Convert the name to UpperCamelCase, lowerCamelCase, and UnderscoreCase
    words = re.split(r'_|(?<!^)(?=[A-Z])', name)
    words = [word.lower() for word in words]

    upper_camel_case = ''.join(word.capitalize() for word in words)
    lower_camel_case = words[0] + ''.join(word.capitalize() for word in words[1:])
    underscore_case = '_'.join(words)

    # Match the string against all cases
    # return string == upper_camel_case or string == lower_camel_case or string == underscore_case
    if string == upper_camel_case:
        return upper_camel_case
    elif string == lower_camel_case:
        return lower_camel_case
    elif string == underscore_case:
        return underscore_case
    return ''


def get_data_stamp() -> str:
    return datetime.now().strftime('%Y%m%d%H%M%S')