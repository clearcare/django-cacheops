# -*- coding: utf-8 -*-
import json
import threading
from funcy import memoize, post_processing, ContextDecorator
from django.db.models.expressions import F
# Since Django 1.8, `ExpressionNode` is `Expression`
try:
    from django.db.models.expressions import ExpressionNode as Expression
except ImportError:
    from django.db.models.expressions import Expression

from .conf import model_name, settings
from .utils import non_proxy, NOT_SERIALIZED_FIELDS, elapsed_timer
from .redis import redis_client, handle_connection_failure, load_script
from .signals import cache_invalidation
from .transaction import queue_when_in_transaction

import core  # NOQA


__all__ = ('invalidate_obj', 'invalidate_model', 'invalidate_all', 'no_invalidation')


@queue_when_in_transaction
@handle_connection_failure
def invalidate_dict(model, obj_dict):
    if no_invalidation.active:
        return
    model = non_proxy(model)

    hash_tag = None
    if settings.CACHEOPS_HASH_CALLBACK:
        # '{' must be escaped. This wraps what the callback returns in a single {}'
        hash_tag = '{{{}}}'.format(eval(settings.CACHEOPS_HASH_CALLBACK.format('obj_dict')))

    print(model._meta.db_table, json.dumps(obj_dict, default=str), hash_tag)

    invalidate = load_script('invalidate')
    with elapsed_timer() as duration:
        deleted = invalidate(
            keys=[hash_tag],
            args=[
                model._meta.db_table,
                json.dumps(obj_dict, default=str),
                hash_tag,
            ],
        )

    cache_invalidation.send(
        sender=model,
        model_name=model_name(model),
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
    conjs_keys = redis_client.keys('conj:%s:*' % model._meta.db_table)
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
