#!/usr/bin/env python
import os, sys, re, shutil
os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'


# Use psycopg2cffi for PyPy
try:
    import psycopg2  # noqa
except ImportError:
    # Fall back to psycopg2cffi
    from psycopg2cffi import compat
    compat.register()


# Set up Django
import django
from django.core.management import call_command
if hasattr(django, 'setup'):
    django.setup()


# Derive test names
names = next((a for a in sys.argv[1:] if not a.startswith('-')), None)
if not names:
    names = 'tests'
elif re.search(r'^\d+', names):
    names = 'tests.tests.IssueTests.test_' + names
elif not names.startswith('tests.'):
    names = 'tests.tests.' + names

# NOTE: we create migrations each time  since they depend on type of database,
#       python and django versions
try:
    if django.VERSION >= (1, 7):
        shutil.rmtree('tests/migrations', True)
        call_command('makemigrations', 'tests', verbosity=2 if '-v' in sys.argv else 0)
    call_command('test', names, failfast='-x' in sys.argv, verbosity=2 if '-v' in sys.argv else 1)
finally:
    if django.VERSION >= (1, 7):
        shutil.rmtree('tests/migrations', ignore_errors=True)
