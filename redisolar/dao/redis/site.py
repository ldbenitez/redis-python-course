from typing import Set

from redisolar.models import Site
from redisolar.dao.base import SiteDaoBase
from redisolar.dao.base import SiteNotFound
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.schema import FlatSiteSchema


class SiteDaoRedis(SiteDaoBase, RedisDaoBase):
    """SiteDaoRedis persists Site models to Redis.

    This class allows persisting (and querying for) Sites in Redis.
    """
    def insert(self, site: Site, **kwargs):
        """Insert a Site into Redis."""
        hash_key = self.key_schema.site_hash_key(site.id)
        site_ids_key = self.key_schema.site_ids_key()
        client = kwargs.get('pipeline', self.redis)
        client.hset(hash_key, mapping=FlatSiteSchema().dump(site))
        client.sadd(site_ids_key, site.id)

    def insert_many(self, *sites: Site, **kwargs) -> None:
        for site in sites:
            self.insert(site, **kwargs)

    def find_by_id(self, site_id: int, **kwargs) -> Site:
        """Find a Site by ID in Redis."""
        hash_key = self.key_schema.site_hash_key(site_id)
        site_hash = self.redis.hgetall(hash_key)

        if not site_hash:
            raise SiteNotFound()

        return FlatSiteSchema().load(site_hash)

    def find_all(self, **kwargs) -> Set[Site]:
        """Find all Sites in Redis."""
        # START Challenge #1
        # Remove this line when you've written code to build `site_hashes`.
        site_hashes = []
        hash_ids_key = self.key_schema.site_ids_key()
        for site_id in self.redis.smembers(hash_ids_key):
            site_hash_key = self.key_schema.site_hash_key(site_id)
            site_hashes.append(self.redis.hgetall(site_hash_key))
        # END Challenge #1

        return {FlatSiteSchema().load(site_hash) for site_hash in site_hashes}
