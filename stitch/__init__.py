"""零售客户身份拼接库。

把 web / mobile / kiosk / inbox 四个渠道的行为事件归并到统一客户画像。
持久化使用 sqlite3（仅标准库）。
"""

from .errors import StitchError
from .stitcher import IdentityStitcher

__all__ = ["IdentityStitcher", "StitchError"]
