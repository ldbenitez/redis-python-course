from typing import List

import redis

from redisolar.dao.base import FeedDaoBase
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.models import MeterReading
from redisolar.schema import MeterReadingSchema


class FeedDaoRedis(FeedDaoBase, RedisDaoBase):
    """Persists and queries MeterReadings in Redis."""
    GLOBAL_MAX_FEED_LENGTH = 10000
    SITE_MAX_FEED_LENGTH = 2440

    def insert(self, meter_reading: MeterReading, **kwargs) -> None:
        pipeline = kwargs.get('pipeline')

        if pipeline is not None:
            self._insert(meter_reading, pipeline)
            return

        p = self.redis.pipeline()
        self._insert(meter_reading, p)
        p.execute()

    def _insert(self, meter_reading: MeterReading,
                pipeline: redis.client.Pipeline) -> None:
        """Helper method to insert a meter reading."""
        meter_reading_dict = MeterReadingSchema().dump(meter_reading)
        pipeline.xadd(self.key_schema.global_feed_key(),
                      meter_reading_dict,
                      maxlen=self.GLOBAL_MAX_FEED_LENGTH)
        pipeline.xadd(self.key_schema.feed_key(meter_reading.site_id),
                      meter_reading_dict,
                      maxlen=self.SITE_MAX_FEED_LENGTH)

    def get_recent_global(self, limit: int, **kwargs) -> List[MeterReading]:
        return self.get_recent(self.key_schema.global_feed_key(), limit)

    def get_recent_for_site(self, site_id: int, limit: int,
                            **kwargs) -> List[MeterReading]:
        return self.get_recent(self.key_schema.feed_key(site_id), limit)

    def get_recent(self, key: str, limit: int) -> List[MeterReading]:
        return [
            MeterReadingSchema().load(entry[1])
            for entry in self.redis.xrevrange(key, count=limit)
        ]
