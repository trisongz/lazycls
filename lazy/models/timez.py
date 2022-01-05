
__all__ = (
    'TimeCls'
)

import time
from typing import Optional, Union, Dict
from .base import BaseDataCls, BaseCls, Field

TimeValues: Dict[str, int] = {
    'secs': 60,
    'mins': 60,
    'hrs': 60,
    'days': 24,
    'wks': 7,
    'mnths': 4
}


class TimeCls(BaseCls):
    t: Optional[Union[float, int]] = Field(default_factory=time.time)
    short: Optional[bool] = False
    start: Optional[Union[float, int]] = None

    @property
    def now(self):
        if self.start: return self.start
        return time.time()

    def stop(self): self.start = time.time()

    @property
    def diff(self): return self.now - self.t
    @property
    def s(self): return self.secs
    @property
    def secs(self): return self.diff
    @property
    def seconds(self): return self.secs
    @property
    def m(self): return self.mins
    @property
    def mins(self): return self.secs / 60
    @property
    def minutes(self): return self.mins
    @property
    def h(self): return self.hrs
    @property
    def hrs(self): return self.mins / 60
    @property
    def hr(self): return self.hrs
    @property
    def hour(self): return self.hrs
    @property
    def hours(self): return self.hrs
    @property
    def d(self): return self.days
    @property
    def day(self): return self.days
    @property
    def days(self): return self.hrs / 24
    @property
    def w(self): return self.wks
    @property
    def wk(self): return self.wks
    @property
    def wks(self): return self.days / 7
    @property
    def week(self): return self.wks
    @property
    def weeks(self): return self.wks
    @property
    def month(self): return self.mnths
    @property
    def mons(self): return self.mnths
    @property
    def mnths(self): return self.wks / 4
    @property
    def months(self): return self.mnths
    
    @property
    def ablstime(self) -> BaseDataCls:
        curr_val = self.secs
        dict_val, str_val = {}, ''
        for tkey, tnum in TimeValues.items():
            if curr_val >= 1:
                tval = tkey[0] if self.short else tkey
                curr_val, curr_num = divmod(curr_val, tnum)
                if type(curr_num) == float: str_val = f'{curr_num:.1f} {tval} ' + str_val
                else: str_val = f'{curr_num} {tval} ' + str_val
                dict_val[tkey] = curr_num
        str_val = str_val.strip()
        return BaseDataCls(string=str_val, value=dict_val, dtype='time')

