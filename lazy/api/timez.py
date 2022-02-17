from __future__ import annotations

import time
from datetime import datetime, timezone
from lazy.models.timez import TimeCls
from .config import TimeZoneConfigz

from .base_imports import *

def _require_pytz():
    pytz = Lib.import_lib('pytz')
    Lib.reload_module(pytz)

def _require_dateparser():
    dateparser = Lib.import_lib('dateparser')
    Lib.reload_module(dateparser)

if _pytz_available:
    import pytz
    api_timezone = pytz.timezone(TimeZoneConfigz.desired)
    utc_timezone = pytz.timezone("UTC")
    dtime_now_tz = lambda: datetime.now(api_timezone)
else:
    api_timezone = _require_pytz
    utc_timezone = _require_pytz
    dtime_now_tz = _require_pytz

if _dateparser_available:
    import dateparser

dtime_now = lambda: datetime.now()
dtime_now_utc = lambda: datetime.now(timezone.utc)

def timer(s: float = None):
    return time.time() if not s else (time.time() - s)


def dtime_parse(timeframe: str = '30 mins', future: bool = False):
    if not _dateparser_available: _require_dateparser()
    if future:
        timeframe = 'in ' + timeframe
        prefer = 'future'
    else:
        timeframe += ' ago'
        prefer = 'past'
    return dateparser.parse(timeframe, settings={'PREFER_DATES_FROM': prefer, 'TIMEZONE': 'UTC', 'RETURN_AS_TIMEZONE_AWARE': True})


def dtime_diff(dtime: datetime = None, timeframe: str = '30 mins', future: bool = False, secs_only: bool = False):
    if not dtime: dtime = datetime.now(timezone.utc)
    dtime_to_obj = dtime_parse(timeframe=timeframe, future=future)
    dtime_diff = (dtime_to_obj - dtime) if future else (dtime - dtime_to_obj)
    if secs_only: return dtime_diff.total_seconds()
    return dtime_diff

def get_dtime(dtime: datetime = None, start: datetime = None, timeframe: str = None, future: bool = False) -> datetime:
    if dtime and start: return start - dtime
    if dtime and timeframe: return dtime_diff(dtobj=dtime, timeframe=timeframe, future=future)
    if timeframe: return dtime_parse(timeframe=timeframe, future=future)
    return dtime_now_utc()

def get_dtime_str(dtime: datetime = None, start: datetime = None, timeframe: str = None, future: bool = False, tz_format: bool = True, dt_format: str = None):
    dt = get_dtime(dtime=dtime, start=start, timeframe=timeframe, future=future)
    if tz_format: return dt.strftime(TimeZoneConfigz.tz_format)
    if dt_format: return dt.strftime(dt_format)
    return dt.isoformat('T')
    
def get_dtime_iso(dtime_str: str, z_break: str = 'Z', z_repl: str  = '.000000+00:00'):
    """Breaks the timestamp at z_break and replaces with z_repl if z_break is not empty. """
    # We use central timezone for rancher clusters, so need to convert CST -> UTC
    if z_break: return datetime.fromisoformat(dtime_str.replace(z_break, z_repl))
    return datetime.fromisoformat(dtime_str).astimezone(utc_timezone)

def get_date(timeframe: str = None, future: bool = False):
    if not timeframe: return dtime_now_utc()
    return dtime_parse(timeframe=timeframe, future=future)

def get_dtime_secs(dtime: datetime = None, start: datetime = None, as_cls: bool = False):
    if dtime and start: return (start - dtime).total_seconds()
    if as_cls: return TimeCls((dtime_now() - dtime).total_seconds())
    try: return (dtime_now_utc() - dtime).total_seconds()
    except: return (dtime_now() - dtime).total_seconds()

dtime = get_dtime
dtstr = get_dtime_str
dtsecs = get_dtime_secs
dtnow = dtime_now
dtnow_utc = dtime_now_utc
get_timestamp = dtime_now
get_timestamp_tz = dtime_now_tz
get_timestamp_utc = dtime_now_utc
dtiso = get_dtime_iso

__all__ = [
    'time',
    'datetime',
    'dtime_now',
    'dtime_now_tz',
    'dtime_now_utc',
    'get_timestamp',
    'get_timestamp_tz',
    'get_timestamp_utc',
    'api_timezone',
    'utc_timezone',
    'timezone_format',
    'timer',
    'dtime_parse',
    'dtime_diff',
    'get_dtime',
    'get_dtime_str',
    'get_dtime_iso',
    'get_date',
    'get_dtime_secs',
    'TimeCls',
]