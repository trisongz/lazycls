import time

from logz import get_logger, get_cls_logger
from lazy.static.timez import TimeValues
from lazy.models import BaseCls

class TimeResult(BaseCls):
    secs: float = 0.0
    mins: float = 0.0
    hrs: float = 0.0
    days: float = 0.0
    weeks: float = 0.0
    months: float = 0.0

    def get_string(self, short: bool = False, ksize: int = 4, nfloat: int = 5):
        s = ''
        d = self.dict()
        for key in {'secs', 'mins', 'hrs', 'days', 'weeks', 'months'}:
            if d.get(key):
                v = str(round(d[key], nfloat))
                k = key[:ksize] if short else key
                s += f' {v} {k}'
        return s.strip()

    @property
    def string(self):
        return self.get_string()

    @property
    def short(self):
        return self.get_string(short=True)


def to_time_string(t: float, short: bool = False, as_obj: bool = False):
    dict_val = {}
    curr_val = t
    for tkey, tnum in TimeValues.items():
        if curr_val > 0:
            curr_val, curr_num = divmod(curr_val, tnum)
            dict_val[tkey] = curr_num
    tcls = TimeResult(**dict_val)
    if as_obj: return tcls
    if short: return tcls.short
    return tcls.string



def timer(start: float = None, as_string: bool = False, short: bool = False, as_obj: bool = False, **kwargs):
    """
    Simple Timer Function that returns time.time 
    or result of start - now
    """
    if not start: return time.time()
    stop = (time.time() - start)
    if not as_string: return stop
    return to_time_string(stop, short=short, as_obj = as_obj)

