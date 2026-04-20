import logging
import time
from functools import wraps

import requests

logger = logging.getLogger(__name__)

_RETRYABLE = (requests.Timeout, requests.ConnectionError, requests.HTTPError)


def with_retry(max_attempts: int = 3, backoff: float = 5.0):
    """對 requests 網路錯誤自動重試，指數退避。"""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except _RETRYABLE as e:
                    last_exc = e
                    if attempt < max_attempts:
                        wait = backoff * attempt
                        logger.warning("%s 第 %d 次失敗，%gs 後重試: %s", fn.__name__, attempt, wait, e)
                        time.sleep(wait)
            raise last_exc
        return wrapper
    return decorator
