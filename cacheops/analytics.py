import json
import time

from requests_futures.sessions import FuturesSession

from .conf import (
    INSIGHTS_ACCOUNT_ID,
    INSIGHTS_BATCH_SIZE,
    INSIGHTS_ENABLED,
    INSIGHTS_INSERT_KEY,
    INSIGHTS_WHITELIST,
)

EVENT_URL = 'https://insights-collector.newrelic.com/v1/accounts/{}/events'.format(INSIGHTS_ACCOUNT_ID)

class InsightsReporter(object):

    session = None
    events = []

    def __init__(self):
        self.session = FuturesSession()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'X-Insert-Key': INSIGHTS_INSERT_KEY,
        })

    def _send_events(self):
        self.session.post(EVENT_URL, data=json.dumps(self.events))
        self.events = []

    def _queue_request(self, action, model_name, cache_key, cache_age=0):
        if self.enabled_for_model(model_name):
            self.events.append({
                'eventType': 'CacheEvent',
                'action': action,
                'app_name': model_name,
                'cache_age': cache_age,
                'cache_key': cache_key,
                'timestamp': time.time(),
            })
            if len(self.events) == INSIGHTS_BATCH_SIZE:
                self._send_events()

    def enabled_for_model(self, model_name):
        return INSIGHTS_ENABLED and model_name in INSIGHTS_WHITELIST

    def cache_hit(self, *args):
        self._queue_request('hit', *args)

    def cache_miss(self, *args):
        self._queue_request('miss', *args)

    def cache_created(self, *args):
        self._queue_request('create', *args)


insights_reporter = InsightsReporter()
