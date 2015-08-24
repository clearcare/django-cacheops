import json
from requests_futures.sessions import FuturesSession

from .conf import (
    INSIGHTS_ACCOUNT_ID,
    INSIGHTS_BATCH_SIZE,
    INSIGHTS_ENABLED,
    INSIGHTS_INSERT_KEY,
)

EVENT_URL = 'https://insights-collector.newrelic.com/v1/accounts/{}/events'.format(INSIGHTS_ACCOUNT_ID)

class InsightsReporter(object):

    session = None
    events = []

    def send_events(self):
        if self.session is None:
            self.session = FuturesSession()
            self.session.headers.update({
                'Content-Type': 'application/json',
                'X-Insert-Key': INSIGHTS_INSERT_KEY,
            })

        self.session.post(EVENT_URL, data=json.dumps(self.events))
        self.events = []

    def batch_request(self, action, model, cache_key):
        if not INSIGHTS_ENABLED:
            return
        self.events.append({
            'eventType': 'CacheEvent',
            'action': action,
            'app_name': model,
            'cache_key': cache_key,
        })
        if len(self.events) == INSIGHTS_BATCH_SIZE:
            self.send_events()

    def cache_hit(self, *args):
        self.batch_request('hit', *args)

    def cache_miss(self, *args):
        self.batch_request('miss', *args)

    def cache_created(self, *args):
        self.batch_request('create', *args)


insights_reporter = InsightsReporter()
