import redis

from segments import app_settings
from segments.models import SegmentMembership


class RedisHelper(object):

    def __init__(self):
        self.redis = redis.StrictRedis.from_url(app_settings.SEGMENTS_REDIS_URI)

    def enqueue_user_membership_add(self, user_id, segment):
        self.redis.sadd('segments:add:' + str(user_id), segment.id)
        self.redis.sadd('segments:refresh_memberships', user_id)

    def enqueue_user_membership_delete(self, user_id, segment):
        self.redis.sadd('segments:delete:' + str(user_id), segment.id)
        self.redis.sadd('segments:refresh_memberships', user_id)

    def diff_segment(self, key_1, key_2):
        return self.redis.sdiff(key_1, key_2)

    def export_segment(self, key, segment):
        # Remove the stale original if it exists
        self.redis.delete(key)

        # Fill in the new set
        member_ids = SegmentMembership.objects.filter(segment=segment).values_list('id', flat=True)
        for id_block in chunk_items(member_ids, 10000):
            self.redis.sadd(key, *set(id_block))

    def export_users(self, key, users):
        # Remove the stale original if it exists
        self.redis.delete(key)

        for id_block in chunk_items(users, 10000):
            self.redis.sadd(key, *set(id_block))

    def cleanup_export(self, prefix, segment):
        key = prefix + ':' + segment.slug
        self.redis.delete(key)


def chunk_items(items, chunk_size):
    for item in range(0, len(items), chunk_size):
        yield items[item:item + chunk_size]
