# -*- coding: utf-8 -*-
import six
from funcy import memoize, merge

from django.conf import settings as base_settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.module_loading import import_string


ALL_OPS = {'get', 'fetch', 'count', 'exists'}


class Settings(object):
    CACHEOPS_REDIS = None
    CACHEOPS_DEFAULTS = {}
    CACHEOPS = {}
    CACHEOPS_LRU = False
    CACHEOPS_DEGRADE_ON_FAILURE = False
    CACHEOPS_CLUSTERED_REDIS = False
    CACHEOPS_REDIS_ENGINE = None
    CACHEOPS_HASH_TAG_CALLBACK = None
    FILE_CACHE_DIR = '/tmp/cacheops_file_cache'
    FILE_CACHE_TIMEOUT = 60*60*24*30

    def __getattribute__(self, name):
        if hasattr(base_settings, name):
            return getattr(base_settings, name)
        return object.__getattribute__(self, name)

settings = Settings()


@memoize
def model_name(model):
    app = model._meta.app_label
    # module_name is fallback for Django 1.5-
    model_name = getattr(model._meta, 'model_name', None) or model._meta.module_name
    return '%s.%s' % (app, model_name)


@memoize
def prepare_profiles():
    """
    Prepares a dict 'app.model' -> profile, for use in model_profile()
    """
    profile_defaults = {
        'ops': (),
        'local_get': False,
        'db_agnostic': True,
    }
    profile_defaults.update(settings.CACHEOPS_DEFAULTS)

    model_profiles = {}
    for app_model, profile in settings.CACHEOPS.items():
        if profile is None:
            model_profiles[app_model.lower()] = None
            continue

        model_profiles[app_model.lower()] = mp = merge(profile_defaults, profile)
        if mp['ops'] == 'all':
            mp['ops'] = ALL_OPS
        # People will do that anyway :)
        if isinstance(mp['ops'], six.string_types):
            mp['ops'] = {mp['ops']}
        mp['ops'] = set(mp['ops'])

        if 'timeout' not in mp:
            raise ImproperlyConfigured(
                'You must specify "timeout" option in "%s" CACHEOPS profile' % app_model)

    return model_profiles

@memoize
def model_profile(model):
    """
    Returns cacheops profile for a model
    """
    model_profiles = prepare_profiles()
    app = model._meta.app_label
    app_model = model_name(model)

    for guess in (app_model, '%s.*' % app, '*.*'):
        profile = model_profiles.get(guess)
        if profile:
            profile['name'] = app_model
            return profile

@memoize
def get_hash_tag_callback():
    func = None
    if base_settings.CACHEOPS_CLUSTERED_REDIS:
        try:
            func = import_string(settings.CACHEOPS_HASH_TAG_CALLBACK)
            assert callable(func)
        except Exception:
            raise Exception("If using clustered Redis you must provide a hashtag callback function.")
    return func
