from __future__ import absolute_import, print_function

import sys
import time

from collections import defaultdict
from optparse import make_option

from django.conf import settings
from django.core.management.base import BaseCommand

from cacheops.redis import load_script, redis_client


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def top(display_count, page_size):

    cards = {}
    start_time = time.time()

    conj_keys = redis_client.scan_iter(match='conj:*', count=page_size)
    for conj_key in conj_keys:
        # This operation is O(1)
        # http://redis.io/commands/SCARD
        card = redis_client.scard(conj_key)
        cards[conj_key] = card

    top_keys = sorted(cards, key=cards.get, reverse=True)
    for conj_key in top_keys[:display_count]:
        print('{}: {:,}'.format(conj_key, cards[conj_key]))

    print('\nkeys={:,} pages={:,} in {:.4}s'.format(
        len(cards),
        len(cards) / page_size,
        time.time() - start_time,
    ))


def gc_conj_key(conj_key, page_size):
    cursor = 0
    stats = defaultdict(int)
    while True:
        cursor, members = redis_client.sscan(conj_key, cursor=cursor, count=page_size)
        response = load_script('gc')(keys=members, args=[conj_key])
        stats['processed'] += response[0]
        stats['deleted'] += response[1]
        stats['bytes'] += response[2]
        if cursor == 0:
            break
    return stats


def gc(page_size):
    cursor = 0
    stats = defaultdict(int)
    while True:
        cursor, conj_keys = redis_client.scan(match='conj:*', count=page_size)
        for conj_key in conj_keys:
            response = gc_conj_key(conj_key, page_size)
            for stat, value in response.iteritems():
                stats[stat] += value
        if cursor == 0:
            break

    print('Processed: {:,}'.format(stats['processed']))
    print('Deleted: {:,}'.format(stats['deleted']))
    print('Freed: {}'.format(sizeof_fmt(stats['bytes'])))


class Command(BaseCommand):
    help = 'Cleanup expired invalidation structures'
    option_list = BaseCommand.option_list + (
        make_option(
            '--page-size',
            dest='page_size',
            action='store',
            default=1000,
            help='Page size to use when calling the scan comands',
        ),
        make_option(
            '--host',
            dest='host',
            help='Override the host setting',
        ),
        make_option(
            '--top',
            dest='top',
            help='Show the top X largest conjunction sets',
        ),
    )

    def handle(self, **options):

        page_size = int(options['page_size'])

        if options['top']:
            top(int(options['top']), page_size)
        else:
            if not settings.CACHEOPS_LRU:
                print('This script is only required when CACHEOPS_LRU is enabled', file=sys.stderr)
                sys.exit(1)

            if options['host']:
                settings.CACHEOPS_REDIS['host'] = options['host']

            gc(page_size)
