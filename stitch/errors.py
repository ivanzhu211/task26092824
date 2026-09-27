"""异常定义。"""


class StitchError(Exception):
    """身份拼接库的所有业务错误（重复id冲突、未知id、非法事件等）。"""
