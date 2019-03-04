from segments.models import Segment, SegmentMembership, SegmentExecutionError
from segments.helpers import RedisHelper
from segments import app_settings
from django.contrib.auth import get_user_model
from celery import task
from time import time
import logging

logger = logging.getLogger(__name__)


@task(name='segments_refresh')
def refresh_segments():
    """Celery task to refresh all segments, with timing information. Writes to the logger."""
    start = time()
    failed = []
    segments = list(Segment.objects.all())
    for s in segments:
        start_seg = time()

        try:
            s.refresh()
        except SegmentExecutionError:
            failed.append(s)

        end_seg = time()
        logger.info("SEGMENTS: Refreshed segment %s (id: %s) in %s milliseconds"
                    % (s.name, s.id, (end_seg - start_seg) * 1000))

    end = time()
    logger.info("SEGMENTS: Successfully refreshed %s segments. Failed to refresh %s segments. Complete in %s seconds"
                % (len(segments)-len(failed), len(failed), end - start))


@task(name='refresh_segment')
def refresh_segment(segment_id):
    """Celery task to refresh an individual Segment."""
    try:
        s = Segment.objects.get(pk=segment_id)
        if app_settings.SEGMENTS_REDIS_URI:
            s.refresh_with_redis()
        else:
            s.refresh()
    except Segment.DoesNotExist:
        logger.exception("SEGMENTS: Unable to refresh segment id %s. DoesNotExist.", segment_id)


@task(name='sync_segment_membership_from_redis')
def sync_segment_membership_from_redis(user_id):
    r = RedisHelper()

    # Retrieve the sets of added and deleted segments
    adds = r.redis.smembers('segments:add:' + str(user_id))
    r.redis.sdel('segments:add:' + str(user_id))
    deletes = r.redis.smembers('segments:delete:' + str(user_id))
    r.redis.sdel('segments:delete' + str(user_id))

    # Perform the adds and deletes
    for add in adds:
        SegmentMembership(id=add, user_id=user_id).save()

    for delete in deletes:
        segment = SegmentMembership.objects.filter(id=delete, user_id=user_id)
        if segment:
            segment.delete()

    # Save the user to trigger any post-save signal hooks
    cls = get_user_model()
    u = cls.objects.get(pk=user_id)
    u.save()


@task(name='refresh_segment_memberships_with_redis')
def refresh_segment_memberships_with_redis():
    """Celery task to refresh segment memberships via Redis"""
    r = RedisHelper()
    for user_id in r.redis.spop('segments:refresh_memberships'):
        sync_segment_membership_from_redis.delay(user_id)


@task(name='user_segments_refresh')
def refresh_user_segments(user_id):
    cls = get_user_model()
    try:
        u = cls.objects.get(pk=user_id)
        u.refresh_segments()
    except cls.DoesNotExist:
        logger.exception("SEGMENTS: Unable to refresh segments for user id %s. %s.DoesNotExist" % (cls.__name__, user_id))
