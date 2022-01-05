__all__ = (
    'TimeValues',
    'DefaultHeaders'
)

from typing import Dict, Any

TimeValues: Dict[str, int] = {
    'secs': 60,
    'mins': 60,
    'hrs': 60,
    'days': 24,
    'wks': 7,
    'mnths': 4
}


DefaultHeaders: Dict[str, Any] = {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}
