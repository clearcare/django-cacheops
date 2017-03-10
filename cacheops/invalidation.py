# -*- coding: utf-8 -*-
import json
import six
import threading
from funcy import memoize, post_processing, ContextDecorator
from django.db.models.expressions import F
# Since Django 1.8, `ExpressionNode` is `Expression`
try:
    from django.db.models.expressions import ExpressionNode as Expression
except ImportError:
    from django.db.models.expressions import Expression

from .conf import settings, get_tag
from .utils import non_proxy, NOT_SERIALIZED_FIELDS, elapsed_timer
from .redis import redis_client, handle_connection_failure, load_script
from .signals import cache_invalidation
from .transaction import queue_when_in_transaction


__all__ = ('invalidate_obj', 'invalidate_model', 'invalidate_all', 'no_invalidation')
from pprint import pprint

def invalidate_python(model, obj_dict):
    db_table = model._meta.db_table
    def _to_str(s):
        if not isinstance(s, six.string_types):
            s = str(s)
            if s in ('True', 'False'):
                s = s.lower()
        return s
    def _conj_cache_key(scheme):
        parts = []
        for field_name in scheme.decode('utf-8').split(','):
            if field_name:  # do we need? can we remove empties?
                parts.append('='.join((field_name, _to_str(obj_dict.get(field_name)))))
        conj_key = 'conj:{}:{}'.format(db_table, '&'.join(parts))
        return conj_key

    schemes = redis_client.smembers('schemes:{}'.format(db_table))
    conj_keys = []
    for scheme in schemes:
        conj_keys.append(_conj_cache_key(scheme))
    if len(conj_keys):
        cache_keys = redis_client.sunion(*conj_keys)
        def _chunks(l, n):
            """Yield successive n-sized chunks from l."""
            range_func = range
            if six.PY2:
                range_func = xrange
            for i in range_func(0, len(l), n):
                yield l[i:i + n]

        for keys in _chunks(list(cache_keys), 100):
            redis_client.delete(*keys)
        # assert False
    # invalidate_lua(model, obj_dict)

def invalidate_lua(model, obj_dict):
    return load_script('invalidate')(args=[
        model._meta.db_table,
        json.dumps(obj_dict, default=str)
    ])

@queue_when_in_transaction
@handle_connection_failure
def invalidate_dict(model, obj_dict):
    if no_invalidation.active:
        return
    model = non_proxy(model)
    import os
    if os.environ.get('NEW') or True:
        invalidate_python(model, obj_dict)
    else:
        invalidate_lua(model, obj_dict)
    return

    invalidate = load_script('invalidate')

    hash_tag = None
    if settings.CACHEOPS_CLUSTERED_REDIS:
        hash_tag = get_tag()(model=model)


    with elapsed_timer() as duration:
        if hash_tag:
            deleted = invalidate(
                keys=[hash_tag],
                args=[
                    model._meta.db_table,
                    json.dumps(obj_dict, default=str),
                    hash_tag,
                ],
            )
        else:
            deleted = invalidate(args=[
                model._meta.db_table,
                json.dumps(obj_dict, default=str)
            ])

    cache_invalidation.send(
        sender=model,
        model_name=model._meta.model_name,
        obj_dict=obj_dict,
        deleted=deleted,
        duration=duration(),
    )


def invalidate_obj(obj):
    """
    Invalidates caches that can possibly be influenced by object
    """
    model = non_proxy(obj.__class__)
    invalidate_dict(model, get_obj_dict(model, obj))


@queue_when_in_transaction
@handle_connection_failure
def invalidate_model(model):
    """
    Invalidates all caches for given model.
    NOTE: This is a heavy artillery which uses redis KEYS request,
          which could be relatively slow on large datasets.
    """
    if no_invalidation.active:
        return
    model = non_proxy(model)
    if settings.CACHEOPS_CLUSTERED_REDIS:
        hash_tag = get_tag()(model=model)
        cache_key = '%sconj:%s:*' % (hash_tag, model._meta.db_table)
    else:
        cache_key = 'conj:%s:*' % model._meta.db_table

    conjs_keys = redis_client.keys(cache_key)
    if conjs_keys:
        cache_keys = redis_client.sunion(conjs_keys)
        redis_client.delete(*(list(cache_keys) + conjs_keys))


@queue_when_in_transaction
@handle_connection_failure
def invalidate_all():
    if no_invalidation.active:
        return
    redis_client.flushdb()


class InvalidationState(threading.local):
    def __init__(self):
        self.depth = 0

class _no_invalidation(ContextDecorator):
    state = InvalidationState()

    def __enter__(self):
        self.state.depth += 1

    def __exit__(self, type, value, traceback):
        self.state.depth -= 1

    @property
    def active(self):
        return self.state.depth

no_invalidation = _no_invalidation()


### ORM instance serialization

@memoize
def serializable_fields(model):
    return tuple(f for f in model._meta.fields
                   if not isinstance(f, NOT_SERIALIZED_FIELDS))

@post_processing(dict)
def get_obj_dict(model, obj):
    for field in serializable_fields(model):
        value = getattr(obj, field.attname)
        if value is None:
            yield field.attname, None
        elif isinstance(value, (F, Expression)):
            continue
        else:
            yield field.attname, field.get_prep_value(value)
