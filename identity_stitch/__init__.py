"""零售客户身份拼接库。"""
from .errors import (
    DuplicateEventError,
    RetractedEventError,
    StitchError,
    UnknownEventError,
    ValidationError,
)
from .model import Event, Profile, SharedDevice, Stats
from .store import IdentityStitcher

__all__ = [
    "DuplicateEventError",
    "Event",
    "IdentityStitcher",
    "Profile",
    "RetractedEventError",
    "SharedDevice",
    "Stats",
    "StitchError",
    "UnknownEventError",
    "ValidationError",
]
