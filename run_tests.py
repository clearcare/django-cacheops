#!/usr/bin/env python
import os, sys, re, shutil
os.environ['DJANGO_SETTINGS_MODULE'] = 'tests.settings'
os.environ['CACHEOPS_DEGRADE_ON_FAILURE'] = 'True'

import django
from django.core.management import call_command

names_prefix = 'tests.tests' if django.VERSION >= (1, 6) else 'tests'
names = next((a for a in sys.argv[1:] if not a.startswith('-')), None)
if not names:
    names = 'tests'
elif re.search(r'^\d+', names):
    names = 'tests.tests.IssueTests.test_' + names
elif not names.startswith('tests.'):
    names = 'tests.tests.' + names

if hasattr(django, 'setup'):
    django.setup()

# NOTE: we create migrations each time  since they depend on type of database,
#       python and django versions
try:
    if django.VERSION >= (1, 7):
        shutil.rmtree('tests/migrations', True)
        call_command('makemigrations', 'tests', verbosity=2 if '-v' in sys.argv else 0)
    call_command('test', names, failfast='-x' in sys.argv, verbosity=2 if '-v' in sys.argv else 1)
finally:
    if django.VERSION >= (1, 7):
        shutil.rmtree('tests/migrations')
