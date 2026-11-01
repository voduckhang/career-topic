"""Phân loại lỗi để common.run_consumer chọn đúng outcome khi settle message."""


class TransientError(Exception):
    """Lỗi tạm thời (DB timeout, service downstream sập...).
    -> FAILED: broker giao lại message."""


class PermanentError(Exception):
    """Message không thể xử lý dù thử lại bao nhiêu lần (sai JSON, thiếu trường...).
    -> REJECTED: broker chuyển message vào DMQ."""