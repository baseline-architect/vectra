from __future__ import annotations

import logging
import random
import time
from functools import wraps
from typing import Callable, Iterable, Type, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., object])


def retry(
    *,
    exceptions: Iterable[Type[BaseException]] = (Exception,),
    max_attempts: int = 5,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
    jitter: float = 0.3,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):  # type: ignore[misc]
            attempt = 0
            delay = base_delay
            while True:
                try:
                    return func(*args, **kwargs)
                except tuple(exceptions) as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error("Operation failed after retries", exc_info=True)
                        raise
                    sleep_time = min(max_delay, delay * (2 ** (attempt - 1)))
                    sleep_time += random.uniform(0, jitter)
                    logger.warning("Retrying after error: %s (attempt %s/%s)", e, attempt, max_attempts)
                    time.sleep(sleep_time)
        return wrapper  # type: ignore[return-value]

    return decorator
