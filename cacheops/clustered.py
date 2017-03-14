# -*- coding: utf-8 -*-
import six
from collections import defaultdict

from django.utils.encoding import smart_text

from .conf import settings, get_hash_tag
from .redis import redis_client, handle_connection_failure
from .utils import extract_hash_tag

__all__ = ('invalidate_clustered', 'cache_thing_clustered')


@handle_connection_failure
def cache_thing_clustered(cache_key, pickled_data, cond_dnfs, timeout):
    """
    Writes data to cache and creates appropriate invalidators
    using python so we can handle multiple shard hash tags.
    """
    hash_tag = extract_hash_tag(cache_key)

    # Write data to cache
    if timeout is not None:
        redis_client.setex(cache_key, timeout, pickled_data)
    else:
        redis_client.set(cache_key, pickled_data)

    for disj_pair in cond_dnfs:
        db_table = disj_pair[0]
        schemes_key = '{}schemes:{}'.format(hash_tag, db_table)
        disj = disj_pair[1]
        for conj in disj:
            # conj is like: ((u'brand_id', 2), (u'label_id', 2))
            conj_scheme = ','.join([p[0] for p in conj])
            # make sure this unique scheme is known
            redis_client.sadd(schemes_key, conj_scheme)

            # Add our cache_key to the right conj key for invalidation
            eq_conjs = ['='.join([p[0], _to_str(p[1])]) for p in conj]
            and_conjs = '&'.join(eq_conjs)
            conj_key = u'{}conj:{}:{}'.format(hash_tag, db_table, and_conjs)
            redis_client.sadd(conj_key, cache_key)

            if not settings.CACHEOPS_LRU:
                conj_ttl = redis_client.ttl(conj_key)
                if conj_ttl < timeout:
                    redis_client.expire(conj_key, timeout * 2 + 10)


def _to_str(s):
    if not isinstance(s, six.string_types):
        s = str(s)
        if s in ('True', 'False'):
            s = s.lower()
    return smart_text(s)


def _conj_cache_key(hash_tag, db_table, scheme, obj_dict):
    parts = []
    for field_name in scheme.decode('utf-8').split(','):
        if field_name:  # do we need? can we remove empties?
            parts.append(
                '='.join(
                    (field_name, _to_str(obj_dict.get(field_name)))))
    conj_key = '{}conj:{}:{}'.format(hash_tag, db_table, '&'.join(parts))
    return conj_key


def _chunks(l, n):
    """Yield successive n-sized chunks from l."""
    range_func = range
    if six.PY2:
        range_func = xrange
    for i in range_func(0, len(l), n):
        yield l[i:i + n]


def _group_keys_by_hash_tag(cache_keys):
    key_groups = defaultdict(list)
    for key in cache_keys:
        hash_tag = extract_hash_tag(key)
        key_groups[hash_tag].append(key)
    return key_groups


def invalidate_clustered(model, obj_dict):
    db_table = model._meta.db_table
    hash_tag = get_hash_tag()(model=model)
    schemes = redis_client.smembers('{}schemes:{}'.format(hash_tag, db_table))
    conj_keys = []
    for scheme in schemes:
        conj_keys.append(_conj_cache_key(
            hash_tag, db_table, scheme, obj_dict))
    num_conj_keys = len(conj_keys)
    num_cache_keys = 0
    if num_conj_keys:
        cache_keys = redis_client.sunion(*conj_keys)
        for hash_tag, grouped_keys in _group_keys_by_hash_tag(cache_keys).iteritems():
            for keys in _chunks(list(grouped_keys), 100):
                num_cache_keys += len(keys)
                redis_client.delete(*keys)
    return num_conj_keys + num_cache_keys
