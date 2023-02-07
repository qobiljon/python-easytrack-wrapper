'''Utility functions for the EasyTrack backend.'''

from datetime import datetime
from os.path import join, exists
from os import mkdir, chmod
import hashlib
from typing import Any, List
import tempfile
import re
from dateutil import parser
import pytz


def replacenull(value: Any, replacement: Any):
    """
    Replaces a None value with a replacement value
    :param value: value being checked
    :param replacement: replacement value
    :return: value if it is not None, otherwise replacement
    """

    if value is None:
        if replacement is None:
            raise ValueError(f'replacement value cannot be {None}')   # is None
        return replacement   # is not None
    return value   # is not None


def notnull(value: Any) -> Any:
    """
    Raises an exception if the provided value is None
    :param value: value being checked
    :return: value if it is not None
    """

    if value is None:
        raise ValueError('Provided argument value is None!')
    return value


def datetime_to_millis(value: datetime) -> int:
    '''Converts a datetime object to an integer timestamp.'''
    return int(round(value.timestamp()*1000))


def millis_to_datetime(value: int) -> datetime:
    '''Converts an integer timestamp to a datetime object.'''
    return datetime.fromtimestamp(value/1000)


def datetime_to_str(timestamp: datetime, js_format: bool) -> str:
    """
    Converts a datetime object to a string.
    :param timestamp: datetime object
    :param js_format: whether to use a JS-compatible format
    :return: string representation of the datetime object
    """
    if timestamp is None or timestamp == 0:
        return "N/A"
    if js_format:
        return timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return timestamp.strftime('%m/%d (%a), %I:%M %p')


def parse_timestamp_str(
    timestamp_str: str,
    add_tz_hours_diff: str = +9,
) -> datetime:
    """
    Parses a timestamp string into a datetime object.
    :param timestamp_str: timestamp string
    :param add_tz_hours_diff: timezone offset in hours
    :return: datetime object
    """

    sign = "-" if add_tz_hours_diff < 0 else "+"
    timezone_part = f'{abs(add_tz_hours_diff):02}00'
    return parser.parse(f'{timestamp_str} {sign}{timezone_part}')


def str_is_numeric(timestamp_str: str, floating = False) -> bool:
    """
    Checks if a string is numeric.
    :param timestamp_str: string being checked
    :param floating: whether to check for floating point numbers
    :return: whether the string is numeric
    """
    if floating:
        return re.search(pattern = r'^[+-]?\d+\.\d+$', string = timestamp_str) is not None
    return re.search(pattern = r'^[+-]?\d+$', string = timestamp_str) is not None


def param_check(request_body, params: List[str]) -> bool:
    """
    Checks if a request body contains all the required parameters.
    :param request_body: request body being checked
    :param params: list of required parameters
    :return: whether the request body contains all the required parameters
    """
    for param in params:
        if param not in request_body:
            return False
    return True


def md5(value: str) -> str:
    '''Returns the md5 hash of a string.'''
    return hashlib.md5(value.encode()).hexdigest()


def get_temp_filepath(filename: str) -> str:
    """
    Returns the path to a temporary file.
    :param filename: name of the file
    :return: path to the file
    """

    root = join(tempfile.gettempdir(), 'easytrack_dashboard')
    if not exists(root):
        mkdir(root)
        chmod(root, 0o777)

    res = join(root, filename)
    with open(res, 'w+', encoding = 'utf8'):
        pass   # create a file if it doesn't exist

    return res


def is_web_ts(timestamp_str: str) -> bool:
    '''Checks if a string is a valid web timestamp.'''
    regex_pattern = r'^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}$'
    return bool(re.search(pattern = regex_pattern, string = timestamp_str))


def strip_tz(value: datetime) -> datetime:
    '''Strips timezone information from a datetime object.'''
    if value.tzinfo:
        return value.astimezone(tz = pytz.utc).replace(tzinfo = None)
    return value
