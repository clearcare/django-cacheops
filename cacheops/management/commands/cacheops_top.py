from __future__ import absolute_import, print_function

import sys
import time

from optparse import make_option

from django.core.management.base import BaseCommand

from cacheops.redis import redis_client


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


def largest_sets(display_count, max_pages, page_size, verbose):

    cards = []
    start_time = time.time()
    current_min = 0

    conj_keys = redis_client.scan_iter(match='conj:*', count=page_size)
    for sampled, conj_key in enumerate(conj_keys, 1):
        card = redis_client.scard(conj_key)

        if len(cards) < display_count:
            cards.append((card, conj_key))
            if current_min == 0:
                current_min = card
            else:
                current_min = min(current_min, card)
        elif card > current_min:
            replace = None
            for i, size in enumerate(cards):
                if size[0] < card:
                    replace = i
                    break
            if replace is not None:
                cards[replace] = (card, conj_key)
                current_min = min(current_min, card)

        pages = sampled / page_size

        if sampled % page_size == 0:
            print_largest_sets(cards, sampled, pages, start_time)

        if max_pages and pages > max_pages:
            break


def print_largest_sets(cards, sampled, pages, page_size, start_time):
    for i, item in enumerate(sorted(cards, reverse=True), 1):
        print('{:<3} {} {:,}'.format(str(i) + ')', item[1], item[0]))

    print('\nkeys={:,} pages={:,} in {}'.format(
        sampled,
        pages,
        pretty_time_delta(time.time() - start_time),
    ))


def largest_keys(display_count, max_pages, page_size):

    sizes = []
    current_min = 0
    pages = 0
    total_bytes = 0

    keys = redis_client.scan_iter(match='q:*', count=page_size)
    for sampled, key in enumerate(keys, 1):
        data = redis_client.get(key)
        if data is None:
            continue
        data_len = len(data)

        total_bytes += data_len

        if len(sizes) < display_count:
            sizes.append((data_len, key))
            if current_min == 0:
                current_min = data_len
            else:
                current_min = min(current_min, data_len)
        elif data_len > current_min:
            replace = None
            for i, size in enumerate(sizes):
                if size[0] < data_len:
                    replace = i
                    break
            if replace is not None:
                sizes[replace] = (data_len, key)
                current_min = min(current_min, data_len)

        pages = sampled / page_size

        if sampled % page_size == 0:
            print_largest_keys(sizes, sampled, pages, total_bytes, total_bytes / sampled)

        if max_pages and pages > max_pages:
            break

    print_largest_keys(sizes, sampled, pages, total_bytes, total_bytes / sampled)


def print_largest_keys(sizes, sampled, pages, total_bytes, avg):
    for i, item in enumerate(sorted(sizes, reverse=True), 1):
        print('{:<3} {} {}'.format(str(i) + ')', item[1], sizeof_fmt(item[0])))
    print('\nAverage key size: {}'.format(sizeof_fmt(avg)))
    print('{:,} keys {} sampled in {:,} pages'.format(sampled, sizeof_fmt(total_bytes), pages))


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
            default=1000,
            help='Page size to use when calling the scan comands',
        ),
        make_option(
            '--keys',
            dest='keys',
            action='store_true',
            help='Find the largest keys',
        ),
        make_option(
            '--sets',
            dest='sets',
            action='store_true',
            help='Find the largest keys',
        ),
        make_option(
            '--display',
            dest='display',
            default=20,
            help='Set the number of items to display',
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

        display_count = int(options['display'])
        page_size = int(options['page_size'])

        if options['sets']:
            largest_sets(display_count, pages, page_size)
        elif options['keys']:
            largest_keys(display_count, pages, page_size)
        else:
            print('Must specify --keys or --sets', file=sys.stderr)
            sys.exit(1)
