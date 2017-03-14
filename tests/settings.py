import os

ROOT_DIR = os.getcwd()

INSTALLED_APPS = [
    'cacheops',
    'django.contrib.contenttypes',
    'django.contrib.auth',
    'tests',
]

ROOT_URLCONF = 'tests.urls'

MIDDLEWARE_CLASSES = []

AUTH_PROFILE_MODULE = 'tests.UserProfile'

# Django replaces this, but it still wants it. *shrugs*
DATABASE_ENGINE = 'django.db.backends.sqlite3',
if os.environ.get('CACHEOPS_DB') == 'postgresql':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'cacheops',
            'USER': 'cacheops',
            'PASSWORD': '',
            'HOST': ''
        },
        'slave': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': 'cacheops_slave',
            'USER': 'cacheops',
            'PASSWORD': '',
            'HOST': ''
        },
    }
elif os.environ.get('CACHEOPS_DB') == 'postgis':
    POSTGIS_VERSION = (2, 1, 1)
    DATABASES = {
        'default': {
            'ENGINE': 'django.contrib.gis.db.backends.postgis',
            'NAME': 'cacheops',
            'USER': 'cacheops',
            'PASSWORD': '',
            'HOST': '',
        },
        'slave': {
            'ENGINE': 'django.contrib.gis.db.backends.postgis',
            'NAME': 'cacheops_slave',
            'USER': 'cacheops',
            'PASSWORD': '',
            'HOST': '',
        },
    }
elif os.environ.get('CACHEOPS_DB') == 'mysql':
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'cacheops',
            'USER': 'root',
            'PASSWORD': '',
            'HOST': '',
        },
        'slave': {
            'ENGINE': 'django.db.backends.mysql',
            'NAME': 'cacheops_slave',
            'USER': 'root',
            'PASSWORD': '',
            'HOST': '',
        },
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': 'sqlite.db',
            # Make in memory sqlite test db to work with threads
            # See https://code.djangoproject.com/ticket/12118
            'TEST': {
                'NAME': os.path.join(ROOT_DIR, 'cacheops_sqlite.db')
            }
        },
        'slave': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': 'sqlite_slave.db',
        }
    }

CACHEOPS_REDIS = {
    'host': 'localhost',
    'port': 6379,
    'db': os.environ.get('DB', 13),
    'socket_timeout': 3,
}
CACHEOPS_DEFAULTS = {
    'timeout': 60*60
}
CACHEOPS = {
    'tests.local': {'local_get': True},
    'tests.cacheonsavemodel': {'cache_on_save': True},
    'tests.dbbinded': {'db_agnostic': False},
    'tests.*': {},
    'tests.noncachedvideoproxy': None,
    'tests.noncachedmedia': None,
    'auth.*': {},
    'tests.polymorphica': {'ops': 'all', 'timeout': 60 * 15},
    'tests.polymorphicb': {'ops': 'all', 'timeout': 60 * 15},
}

CACHEOPS_LRU = bool(os.environ.get('CACHEOPS_LRU'))
CACHEOPS_DEGRADE_ON_FAILURE = bool(os.environ.get('CACHEOPS_DEGRADE_ON_FAILURE'))
ALLOWED_HOSTS = ['testserver']

SECRET_KEY = 'abc'

# Required in Django 1.9
TEMPLATES = [{'BACKEND': 'django.template.backends.django.DjangoTemplates'}]
# CACHEOPS_HASH_TAG_CALLBACK = 'tests.models.hash_tag_callback'
CACHEOPS_HASH_TAG_CALLBACK = 'tests.clustered.hash_tag_callback'
if os.environ.get('TEST_CLUSTERED'):
    CACHEOPS_CLUSTERED_REDIS = True
