import datetime
import random
from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema
from redisolar.dao.base import RateLimitExceededException


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""
    def __init__(self,
                 window_size_ms: float,
                 max_hits: int,
                 redis_client: Redis,
                 key_schema: KeySchema = None,
                 **kwargs):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        pipeline = self.redis.pipeline()
        key = self.key_schema.sliding_window_rate_limiter_key(name, self.window_size_ms,
                                                              self.max_hits)

        now = datetime.datetime.utcnow().timestamp() * 1000
        random_number = random.randint(0, self.max_hits)
        pipeline.zadd(key, {f"{str(now)}-{random_number}": now})
        pipeline.zremrangebyscore(key, 0, now - self.window_size_ms)
        pipeline.zcard(key)

        _, _, hits = pipeline.execute()
        if hits > self.max_hits:
            raise RateLimitExceededException()
