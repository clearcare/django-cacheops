#!/usr/bin/env python

import redis

active = 0
empty = 0
tsize = 0

r = redis.StrictRedis(host='localhost', port=6379, db=1)
members = r.smembers('conj:agency.agency:active=True&subdomain=metzler')
for key in members:
    item = r.get(key)
    if item is None:
        empty += 1
    else:
        active += 1
        size = r.strlen(key)
        print('%s: %s' % (key, size))
        tsize += size

print('Empty: %s' % (empty))
print('Active: %s' % (active))
print('Total Size: %s' % (tsize))
