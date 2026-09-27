"""异常类型定义。"""


class StitchError(Exception):
    """本库所有错误的基类。"""


class ValidationError(StitchError, ValueError):
    """事件或标识符格式不合法。"""


class DuplicateEventError(StitchError):
    """同一 event_id 已存在且内容不同。"""


class RetractedEventError(StitchError):
    """该 event_id 已被撤回，不允许再次写入。"""


class UnknownEventError(StitchError):
    """撤回的 event_id 未知。"""
