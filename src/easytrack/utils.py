from datetime import datetime as dt
import dateutil.parser as parser
from os.path import join, exists
from typing import Any, List
from os import mkdir, chmod
import tempfile
import hashlib
import re

import pytz


def replacenull(value: Any, replacement: Any):
  """
	Validate that a provided argument is not None, replace if so.
	Raises a ValueError if both value and replacement are None.
	:param value: value being checked for None
	:param replacement: replacement if value is None
	:return: value or replacement depending on value's content
	"""

  if value is None:
    if replacement is None:
      raise ValueError(f'replacement value cannot be {None}')
    else:
      return replacement   # is None
  else:
    return value   # is not None


def notnull(value: Any) -> Any:
  """
	Asserts that a provided argument is not None
	:param value: value being checked
	:return: value if it is not None
	"""

  if value is None: raise ValueError('Provided argument value is None!')
  return value


def ts2int(value: dt) -> int:
  return int(round(value.timestamp()*1000))


def int2ts(value: int) -> dt:
  return dt.fromtimestamp(value/1000)


def ts2str(ts: dt) -> str:
  if ts is None or ts == 0:
    return "N/A"
  else:
    return ts.strftime('%m/%d (%a), %I:%M %p')


def ts2web(ts: dt) -> str:
  return ts.strftime('%Y-%m-%dT%H:%M')


def parse_ts(s: str) -> dt:
  """
	Converts a given string to a datetime object
	:param s: timestamp in string format
	:return: datetime object
	"""

  return parser.parse(f'{s} +0900')


def is_numeric(s: str, floating = False) -> bool:
  if floating:
    return re.search(pattern = r'^[+-]?\d+\.\d+$', string = s) is not None
  else:
    return re.search(pattern = r'^[+-]?\d+$', string = s) is not None


def param_check(request_body, params: List[str]) -> bool:
  for param in params:
    if param not in request_body:
      return False
  return True


def now_dt() -> dt:
  return dt.now()


def now_us() -> int:
  return int(now_dt().timestamp()*1000*1000)


def now_ms() -> int:
  return int(now_dt().timestamp()*1000)


def md5(value: str) -> str:
  return hashlib.md5(value.encode()).hexdigest()


def get_temp_filepath(filename: str) -> str:
  """
	Validates presence of a temporary directory, and opens a file for writing in the directory.
	:param filename: filename for writing
	:return: path to the file
	"""

  root = join(tempfile.gettempdir(), 'easytrack_dashboard')
  if not exists(root):
    mkdir(root)
    chmod(root, 0o777)

  res = join(root, filename)
  fp = open(res, 'w+', encoding = 'utf8')
  fp.close()

  return res


def is_web_ts(s: str) -> bool:
  """
	Checks if the provided string has a valid datetime format
	:param s: string being validated
	:return: whether string is a valid timestsamp
	"""
  return bool(re.search(pattern = r'^\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}$', string = s))


def strip_tz(ts: dt) -> dt:
  if ts.tzinfo:
    return ts.astimezone(tz = pytz.utc).replace(tzinfo = None)
  return ts
