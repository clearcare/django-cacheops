from __future__ import absolute_import, print_function

import sys

from collections import defaultdict
from optparse import make_option

from django.core.management.base import BaseCommand
from cacheops.redis import redis_client


def iter_csv(csvfile):
    import csv
    with open(csvfile, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield(row[1])


class Command(BaseCommand):
    # option_list = BaseCommand.option_list + (
    #     make_option(
    #         '--flushall',
    #         action='store_true',
    #         default=False,
    #         dest='flushall',
    #         help='Flush all the keys.',
    #     ),
    #     make_option(
    #         '--lookup',
    #         dest='lookup',
    #         help='Lookup the node for a hash slot.',
    #     ),
    #     make_option(
    #         '--csv',
    #         dest='csv',
    #     ),
    # )

    def handle(self, *args, **options):

        d = defaultdict(list)

        if options.get('flushall'):
            redis_client.flushall()
            sys.exit(1)

        if options.get('lookup'):
            redis_client.connection_pool.nodes.node_from_slot(options.get('lookup'))
            sys.exit(1)

        if options.get('csv'):
            for subdomain in iter_csv(options.get('csv')):
                key = '{{a:{}}}'.format(subdomain)
                keyslot = redis_client.connection_pool.nodes.keyslot(key)
                node = redis_client.connection_pool.nodes.node_from_slot(keyslot)['name']
                d[node].append(key)

            for k, l in d.iteritems():
                print(k, len(l))

            sys.exit(1)

        keys = redis_client.scan_iter()
        for key in keys:
            hashtag = redis_client.connection_pool.nodes.hashtag(key)
            keyslot = redis_client.connection_pool.nodes.keyslot(key)
            node = redis_client.connection_pool.nodes.node_from_slot(keyslot)
            d[(hashtag, keyslot, node['name'])].append(key)

        for k, l in d.iteritems():
            print(k, len(l))
