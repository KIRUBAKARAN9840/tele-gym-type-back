# middleware/rate_limit_middleware.py
import time, re, logging
from typing import Dict, Optional, Tuple, List
from fastapi import Request
from fastapi.responses import JSONResponse
import redis.asyncio as redis

logger = logging.getLogger(__name__)

# ---- Helper: correct client IP behind ALB (ALB appends client IP at the END)
def get_real_client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[-1].strip()
    xri = request.headers.get("x-real-ip")
    if xri:
        return xri.strip()
    return request.client.host if request.client else "unknown"

# ---- Window helpers (align expiry to real boundaries)
def _now() -> int:
    return int(time.time())

def _window_secs(now: int) -> Dict[str, int]:
    return {
        "min": 60 - (now % 60),
        "hour": 3600 - (now % 3600),
        "day": 86400 - (now % 86400),
    }

def _burst_secs(now: int, burst_window: int) -> int:
    return burst_window - (now % burst_window)

class IPRateLimitMiddleware:
    """
    Generic subject limiter (works for IP or user). Uses Redis keys per time bucket:
      - subject:m:<bucket>, subject:h:<bucket>, subject:d:<bucket>, subject:b:<bucket>
    Enforces: per-minute, per-hour, per-day and burst window.
    """
    def __init__(
        self,
        redis_client: redis.Redis,
        requests_per_minute: int = 60,
        requests_per_hour: int = 1000,
        requests_per_day: int = 10000,
        burst_limit: int = 10,
        burst_window: int = 10,
        whitelist_subjects: Optional[List[str]] = None,
    ):
        self.redis = redis_client
        self.requests_per_minute = requests_per_minute
        self.requests_per_hour = requests_per_hour
        self.requests_per_day = requests_per_day
        self.burst_limit = burst_limit
        self.burst_window = max(1, burst_window)
        self.whitelist = set(whitelist_subjects or [])

    async def is_subject_limited(self, subject: str) -> Tuple[bool, Dict]:
        """
        subject can be an IP ("1.2.3.4") or a user key ("user:123").
        Returns (is_limited, info dict with counts, limits, retry_after).
        """
        if subject in self.whitelist:
            return False, {"whitelisted": True}

        now = _now()
        secs = _window_secs(now)
        bsecs = _burst_secs(now, self.burst_window)

        # Keys for current buckets
        mkey = f"rl:{subject}:m:{now // 60}"
        hkey = f"rl:{subject}:h:{now // 3600}"
        dkey = f"rl:{subject}:d:{now // 86400}"
        bkey = f"rl:{subject}:b:{now // self.burst_window}"

        # Pipeline (atomic enough; Lua optional for strictness)
        pipe = self.redis.pipeline()
        pipe.incr(mkey); pipe.expire(mkey, secs["min"] + 2)
        pipe.incr(hkey); pipe.expire(hkey, secs["hour"] + 5)
        pipe.incr(dkey); pipe.expire(dkey, secs["day"] + 10)
        pipe.incr(bkey); pipe.expire(bkey, bsecs + 1)
        res = await pipe.execute()

        mcount, hcount, dcount, bcount = res[0], res[2], res[4], res[6]

        info = {
            "minute_count": int(mcount),
            "hour_count": int(hcount),
            "day_count": int(dcount),
            "burst_count": int(bcount),
            "limits": {
                "per_minute": self.requests_per_minute,
                "per_hour": self.requests_per_hour,
                "per_day": self.requests_per_day,
                "burst": self.burst_limit,
                "burst_window": self.burst_window,
            },
            "retry_after": 0,
            "tripped": [],
        }

        # Evaluate in "strictest-first" order
        limited = False
        retry_after = 0

        if bcount > self.burst_limit:
            limited = True
            info["tripped"].append("burst")
            retry_after = max(retry_after, bsecs)

        if mcount > self.requests_per_minute:
            limited = True
            info["tripped"].append("minute")
            retry_after = max(retry_after, secs["min"])

        if hcount > self.requests_per_hour:
            limited = True
            info["tripped"].append("hour")
            retry_after = max(retry_after, secs["hour"])

        if dcount > self.requests_per_day:
            limited = True
            info["tripped"].append("day")
            retry_after = max(retry_after, secs["day"])

        info["retry_after"] = int(retry_after)
        return limited, info

    # Backwards-compatible methods if you used IP-specific naming:
    async def is_rate_limited(self, ip: str) -> Tuple[bool, Dict]:
        return await self.is_subject_limited(subject=ip)

    def get_client_ip(self, request: Request) -> str:
        return get_real_client_ip(request)

class EndpointSpecificRateLimit:
    """
    Regex-based endpoint limiter. Matches first pattern, counts per IP+pattern,
    and enforces per-minute/hour limits aligned to window ends.
    """
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.rules: List[Tuple[re.Pattern, Dict[str, int]]] = [
            (re.compile(r"^/auth/login$"),           {"per_minute": 15,  "per_hour": 60}),
            (re.compile(r"^/auth/refresh\?"),        {"per_minute": 300, "per_hour": 1200}),
            (re.compile(r"^/auth/refresh$"),         {"per_minute": 120, "per_hour": 600}),
            (re.compile(r"^/auth/register$"),        {"per_minute": 10,  "per_hour": 30}),
            (re.compile(r"^/auth/forgot-password$"), {"per_minute": 5,   "per_hour": 15}),
            (re.compile(r"^/auth/verify-otp$"),      {"per_minute": 15,  "per_hour": 45}),
            (re.compile(r"^/auth/verify\?"),         {"per_minute": 600, "per_hour": 2400}),
            (re.compile(r"^/auth/verify$"),          {"per_minute": 300, "per_hour": 1200}),
            (re.compile(r"^/feed($|/)"),             {"per_minute": 30,  "per_hour": 500}),
            (re.compile(r"^/food_scanner($|/)"),     {"per_minute": 10,  "per_hour": 100}),
            (re.compile(r"^/ai($|/)"),               {"per_minute": 20,  "per_hour": 200}),
        ]

    def _match(self, path: str) -> Tuple[Optional[str], Optional[Dict[str, int]]]:
        for rx, limits in self.rules:
            if rx.search(path):
                return rx.pattern, limits
        return None, None

    async def check(self, path: str, client_ip: str) -> Tuple[bool, Dict]:
        pattern, limits = self._match(path)
        if not pattern:
            return False, {}

        now = _now()
        secs = _window_secs(now)

        mk = f"epl:{client_ip}:{pattern}:m:{now // 60}"
        hk = f"epl:{client_ip}:{pattern}:h:{now // 3600}"

        pipe = self.redis.pipeline()
        pipe.incr(mk); pipe.expire(mk, secs["min"] + 2)
        pipe.incr(hk); pipe.expire(hk, secs["hour"] + 5)
        res = await pipe.execute()

        mcount, hcount = int(res[0]), int(res[2])
        blocked = mcount > limits["per_minute"] or hcount > limits["per_hour"]

        info = {
            "minute_count": mcount,
            "hour_count": hcount,
            "limits": limits,
            "retry_after": int(max(secs["min"] if mcount > limits["per_minute"] else 0,
                                   secs["hour"] if hcount > limits["per_hour"] else 0)),
        }
        return blocked, {"pattern": pattern, "limits": limits, "info": info}
