VERSION = (3, 1, 2)
CC_VERSION = 'cc8'
__version__ = '{}-{}'.format(
    '.'.join(map(str, VERSION if VERSION[-1] else VERSION[:2])), CC_VERSION)

import django

from .simple import *
from .query import *
from .invalidation import *
from .templatetags.cacheops import *
from .transaction import install_cacheops_transaction_support

# Use app config for initialization in Django 1.7+

if django.VERSION >= (1, 7):
    from django.apps import AppConfig

    class CacheopsConfig(AppConfig):
        name = 'cacheops'

        def ready(self):
            install_cacheops()
            install_cacheops_transaction_support()

    default_app_config = 'cacheops.CacheopsConfig'
else:
    install_cacheops()
