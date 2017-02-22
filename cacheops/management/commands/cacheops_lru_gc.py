"""
If CACHEOPS_LRU is enabled CacheOps will not reap its invalidation structures.
For example:

    > SCARD conj:auth_user:is_active=true
    (integer) 2851530

Shows that the set contains about 3m members. Checking one of the keys from the
set:

    > SSCAN "conj:auth_user:is_active=true" 0 COUNT 10
    1) "1179648"
    2)  1) "q:e0ab9bf9f2e1f1475f663bfe8a50269f"
        2) "q:b09c2d45f348a19578154fcbdd1c07d6"
        3) "as:6a89b161b51cedb7cea37c8e51480465"
        4) "q:cc6c466aef5822649fce5ed738646bb4"
        5) "q:9036ac3b3880fb47f36e37451837eec2"
        6) "q:5c8903f26de59ba20e47bbf7f0a7e70a"
        7) "q:8694b66aad2186e03b940580f8d1b00c"
        8) "q:a083362753b9c5e986f3c10c0dddc19d"
        9) "q:1e13437f509ab6bb8ad51ab19d30f1d9"
       10) "q:e19c6b853a0f979bd28f65bc2c705b85"
       11) "q:5f02f2160186282d111fa18722ece7ed"
    > EXISTS "q:e0ab9bf9f2e1f1475f663bfe8a50269f"
    (integer) 0

Shows that the key has been expired by cacheops but has NOT been removed from
the set. This script will iterate through all the conjuction sets and call
SREM on the the members that do not exist. If the resulting set has no members
it is also deleted.

Below is the current output from the `cacheops_lru_gc` when it has completed:

    Processed: 71,349,660
    Deleted Items: 57,674,051
    Deleted Sets: 11,637,743
    Deleted %: 97.14
    Bytes per second: 89.3KiB
    Items per second: 2519.46
    Errors: 0
    Pages: 12,373,159
    Freed: 2.4GiB
    Time: 7h471m59s0.46ms

The `cacheops_top` command will show the top 20 keys with largest values
(pickled querysets) or the top 20 sets with the most number of members.
 Depending on whether `--keys` or `--sets` is passed.
"""
from __future__ import absolute_import, print_function


import json
import logging
import sys
import time

from collections import defaultdict
from optparse import make_option

from django.conf import settings
from django.core.management.base import BaseCommand

from cacheops.redis import load_script, redis_client
from cacheops.management.util import pretty_time_delta, sizeof_fmt


logger = logging.getLogger(__name__)


def gc_conj_key(conj_key, max_pages, page_size, interval, wait_pages):
    cursor = 0
    pages = 0
    stats = defaultdict(int)
    start_time = time.time()
    while True:
        cursor, members = redis_client.sscan(conj_key, cursor=cursor, count=page_size)
        response = load_script('gc')(keys=members, args=[conj_key])
        stats['processed'] += response[0]
        stats['deleted_items'] += response[1]
        stats['deleted_sets'] += response[2]
        stats['errors'] += response[3]
        stats['bytes'] += response[4]
        pages += 1
        if cursor == 0 or (max_pages is not None and max_pages > pages):
            break
        if pages % wait_pages:
            time.sleep(interval)
    stats['runtime'] = time.time() - start_time
    stats['pages'] = pages
    return stats


def gc(max_pages, page_size, interval, verbosity, wait_pages):
    cursor = 0
    pages = 0
    stats = defaultdict(int)
    start_time = time.time()
    while True:
        cursor, conj_keys = redis_client.scan(cursor=cursor, match='conj:*', count=page_size)
        for conj_key in conj_keys:
            conj_stats = gc_conj_key(conj_key, pages, page_size, interval, wait_pages)
            stats['processed'] += conj_stats['processed']
            stats['deleted_items'] += conj_stats['deleted_items']
            stats['deleted_sets'] += conj_stats['deleted_sets']
            stats['errors'] += conj_stats['errors']
            stats['bytes'] += conj_stats['bytes']
            stats['runtime'] = time.time() - start_time
            stats['bps'] = stats['bytes'] / float(stats['runtime'])
            stats['ips'] = stats['processed'] / float(stats['runtime'])
            stats['pages'] += conj_stats['pages']
        pages += 1
        if verbosity and pages % 100:
            print_stats(stats)
        if cursor == 0 or (max_pages is not None and max_pages > pages):
            break
    return stats


def print_stats(stats):

    print('Processed: {:,}'.format(stats['processed']))
    print('Deleted Items: {:,}'.format(stats['deleted_items']))
    print('Deleted Sets: {:,}'.format(stats['deleted_sets']))
    print('Deleted %: {:.2f}'.format(
        ((stats['deleted_items'] + stats['deleted_sets']) / float(stats['processed'])) * 100
    ))
    print('Bytes per second: {}'.format(sizeof_fmt(stats['bps'])))
    print('Items per second: {:.2f}'.format(stats['ips']))
    print('Errors: {:,}'.format(stats['errors']))
    print('Pages: {:,}'.format(stats['pages']))
    print('Freed: {}'.format(sizeof_fmt(stats['bytes'])))
    print('Time: {}'.format(pretty_time_delta(stats['runtime'])))
    print('')


def log_stats(stats):
    logger.info(json.dumps(stats))


class Command(BaseCommand):
    help = 'Cleanup expired invalidation structures'
    option_list = BaseCommand.option_list + (
        make_option(
            '--pages',
            dest='pages',
            help='Max number of pages to use when calling the scan comands',
        ),
        make_option(
            '--page-size',
            dest='page_size',
            action='store',
            default=1000,
            help='Page size to use when calling the scan comands',
        ),
        make_option(
            '--conj',
            dest='conj',
            help='The conj set to process',
        ),
        make_option(
            '--interval',
            dest='interval',
            default=.100,
            help='The time to wait between sweeps.',
        ),
        make_option(
            '--wait-pages',
            dest='wait_pages',
            action='store',
            default=100,
            help='Number of pages to process before sleeping',
        ),
        make_option(
            '--host',
            dest='host',
            help='Override the host setting.',
        ),
    )

    def handle(self, *args, **options):

        pages = options['pages']
        if pages is not None:
            pages = int(pages)

        page_size = int(options['page_size'])
        wait_pages = int(options['wait_pages'])
        interval = float(options['interval'])
        verbosity = options['verbosity'] > 1

        if options['conj']:
            stats = gc_conj_key(options['conj'], pages, page_size, interval, wait_pages)
            print_stats(stats)
        else:
            if not settings.CACHEOPS_LRU:
                print('This script is only required when CACHEOPS_LRU is enabled', file=sys.stderr)
                sys.exit(1)

            if options['host']:
                settings.CACHEOPS_REDIS['host'] = options['host']

            stats = gc(pages, page_size, interval, verbosity, wait_pages)
            if verbosity:
                print_stats(stats)
            else:
                log_stats(stats)
