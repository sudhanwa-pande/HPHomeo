from collections.abc import Callable

from fastapi import Depends
from fastapi_limiter.depends import RateLimiter


def rl(times: int, seconds: int, *, identifier: Callable | None = None):
    return Depends(RateLimiter(times=times, seconds=seconds, identifier=identifier))
