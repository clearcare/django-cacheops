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


def pretty_time_delta(seconds):
    # https://gist.github.com/thatalextaylor/7408395
    int_seconds = int(seconds)
    milliseconds = seconds - int_seconds
    days, seconds = divmod(int_seconds, 86400)
    hours, seconds = divmod(int_seconds, 3600)
    minutes, seconds = divmod(int_seconds, 60)
    if days > 0:
        return '{}d{}h{}m{}s{:0.2f}ms'.format(days, hours, minutes, seconds, milliseconds)
    if hours > 0:
        return '{}h{}m{}s{:0.2f}ms'.format(hours, minutes, seconds, milliseconds)
    if minutes > 0:
        return '{}m{}s{:0.2f}ms'.format(minutes, seconds, milliseconds)
    return '{}s{:0.2f}ms'.format(seconds, milliseconds)


def top(display_count, pages, page_size):

    cards = {}
    start_time = time.time()

    conj_keys = redis_client.scan_iter(match='conj:*', count=page_size)
    for i, conj_key in enumerate(conj_keys):
        # This operation is O(1)
        # http://redis.io/commands/SCARD
        card = redis_client.scard(conj_key)
        cards[conj_key] = card
        if pages and i % page_size > pages:
            break

    top_keys = sorted(cards, key=cards.get, reverse=True)
    for conj_key in top_keys[:display_count]:
        print('{}: {:,}'.format(conj_key, cards[conj_key]))

    print('\nkeys={:,} pages={:,} in {}'.format(
        len(cards),
        len(cards) / page_size,
        pretty_time_delta(time.time() - start_time),
    ))


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
            stats['pages'] = pages
        pages += 1
        if verbosity > 1 and pages % 100:
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
        make_option(
            '--top',
            dest='top',
            help='Show the top X largest conjunction sets.',
        ),
    )

    def handle(self, *args, **options):

        pages = options['pages']
        if pages is not None:
            pages = int(pages)

        page_size = int(options['page_size'])
        wait_pages = int(options['wait_pages'])
        interval = float(options['interval'])
        verbosity = options['verbosity']

        if options['top']:
            top(int(options['top']), pages, page_size)
        elif options['conj']:
            stats = gc_conj_key(options['conj'], pages, page_size, interval, wait_pages)
            print_stats(stats)
        else:
            if not settings.CACHEOPS_LRU:
                print('This script is only required when CACHEOPS_LRU is enabled', file=sys.stderr)
                sys.exit(1)

            if options['host']:
                settings.CACHEOPS_REDIS['host'] = options['host']

            stats = gc(pages, page_size, interval, verbosity, wait_pages)
            print_stats(stats)
