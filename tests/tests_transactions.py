# -*- coding: utf-8 -*-
import unittest
import django
from django.db.transaction import atomic
from django.test import TransactionTestCase

from cacheops.transaction import queue_when_in_transaction

from .models import Category
from .utils import run_in_thread


def get_category():
    return Category.objects.cache().get(pk=1)


class IntentionalRollback(Exception):
    pass


@unittest.skipUnless(django.VERSION >= (1, 7), "transactions supported only in Django 1.7+")
class TransactionSupportTests(TransactionTestCase):
    fixtures = ['basic']

    def test_atomic(self):
        with atomic():
            obj = get_category()
            obj.title = 'Changed'
            obj.save()
            self.assertEqual('Changed', get_category().title)
            self.assertEqual('Django', run_in_thread(get_category).title)
        self.assertEqual('Changed', run_in_thread(get_category).title)
        self.assertEqual('Changed', get_category().title)

    def test_nested(self):
        with atomic():
            with atomic():
                obj = get_category()
                obj.title = 'Changed'
                obj.save()
                self.assertEqual('Changed', get_category().title)
                self.assertEqual('Django', run_in_thread(get_category).title)
            self.assertEqual('Changed', get_category().title)
            self.assertEqual('Django', run_in_thread(get_category).title)
        self.assertEqual('Changed', run_in_thread(get_category).title)
        self.assertEqual('Changed', get_category().title)

    def test_rollback(self):
        try:
            with atomic():
                obj = get_category()
                obj.title = 'Changed'
                obj.save()
                self.assertEqual('Changed', get_category().title)
                self.assertEqual('Django', run_in_thread(get_category).title)
                raise IntentionalRollback()
        except IntentionalRollback:
            pass
        self.assertEqual('Django', get_category().title)
        self.assertEqual('Django', run_in_thread(get_category).title)

    def test_nested_rollback(self):
        with atomic():
            try:
                with atomic():
                    obj = get_category()
                    obj.title = 'Changed'
                    obj.save()
                    self.assertEqual('Changed', get_category().title)
                    self.assertEqual('Django', run_in_thread(get_category).title)
                    raise IntentionalRollback()
            except IntentionalRollback:
                pass
            self.assertEqual('Django', get_category().title)
            self.assertEqual('Django', run_in_thread(get_category).title)
        self.assertEqual('Django', get_category().title)
        self.assertEqual('Django', run_in_thread(get_category).title)

    def test_smart_transactions(self):
        with atomic():
            get_category()
            with self.assertNumQueries(0):
                get_category()
            with atomic():
                with self.assertNumQueries(0):
                    get_category()

            obj = get_category()
            obj.title += ' changed'
            obj.save()

            get_category()
            with self.assertNumQueries(1):
                get_category()

    @unittest.skipIf(not hasattr(django.db.connection, 'on_commit'),
                     'No on commit hooks support (Django < 1.9)')
    def test_call_cacheops_cbs_before_on_commit_cbs(self):
        calls = []

        with atomic():
            def django_commit_handler():
                calls.append('django')
            django.db.connection.on_commit(django_commit_handler)

            @queue_when_in_transaction
            def cacheops_commit_handler():
                calls.append('cacheops')
            cacheops_commit_handler()

        self.assertEqual(calls, ['cacheops', 'django'])
