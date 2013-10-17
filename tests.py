#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gevent import sleep
from random import random
from re import compile
from requests import Response, Session
from time import time
from types import MethodType
from unittest import main, TestCase

from simple_requests import *

class NoRaiseServerErrorIterator(ResponseIterator):
    def error(self, exception):
        if isinstance(exception, HTTPError):
            return exception.response
        else:
            raise exception

class TestLogic(TestCase):
    def setUp(self):
        self.default = Requests()
        self.highConcurrency = Requests(concurrent = 5)
        self.noRaise = Requests(responseIterator = NoRaiseServerErrorIterator)

        parser = compile('^(.+)/([^:]+):([0-9]+):?([.0-9]+)?$')

        # Monkey patch the actual send to make testing timings easier
        def fake_send(self, request):
            g = parser.match(request.url).groups()

            response = Response()
            response.url = g[0]
            response.reason = g[1]
            response.status_code = int(g[2])
            if g[3] is not None:
                wait = float(g[3])
            else:
                wait = 0.4

            sleep(wait)

            if response.status_code >= 600:
                # Special case for testing exception handling
                raise Exception('[%d] %s' % ( response.status_code, response.reason ))

            return response
        self.default.session.send = MethodType(fake_send, self.default.session, Session)
        self.highConcurrency.session.send = MethodType(fake_send, self.highConcurrency.session, Session)
        self.noRaise.session.send = MethodType(fake_send, self.noRaise.session, Session)

    def t1est_sync(self):
        start = time()

        self.assertEqual(self.default.one('http://cat-videos.net/1/OK:200').url, 'http://cat-videos.net/1')
        self.assertEqual(self.default.one('http://cat-videos.net/2/OK:200').url, 'http://cat-videos.net/2')
        self.assertEqual(self.default.one('http://cat-videos.net/3/OK:200').url, 'http://cat-videos.net/3')
        self.assertEqual(self.default.one('http://cat-videos.net/4/OK:200').url, 'http://cat-videos.net/4')
        self.assertEqual(self.default.one('http://cat-videos.net/5/OK:200').url, 'http://cat-videos.net/5')

        self.assertAlmostEqual(time() - start, 2.0, delta = 0.04)

    def t1est_async(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, 1.2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)

    def t1est_async_high_mintime(self):
        responses = []
        oldValue = self.default.minSecondsBetweenRequests
        self.default.minSecondsBetweenRequests = 0.25
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, 1.4, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)
        self.default.minSecondsBetweenRequests = oldValue

    def t1est_async_high_concurrency(self):
        responses = []
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200', 'http://cat-videos.net/6/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, 1.15, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5', 'http://cat-videos.net/6' ], responses)

    def t1est_async_low_mintime1(self):
        responses = []
        oldValue = self.highConcurrency.minSecondsBetweenRequests
        self.highConcurrency.minSecondsBetweenRequests = 0.05
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, 0.6, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)
        self.highConcurrency.minSecondsBetweenRequests = oldValue

    def t1est_async_low_mintime2(self):
        responses = []
        oldValue = self.highConcurrency.minSecondsBetweenRequests
        self.highConcurrency.minSecondsBetweenRequests = 0.05
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200', 'http://cat-videos.net/6/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, 0.8, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5', 'http://cat-videos.net/6' ], responses)
        self.highConcurrency.minSecondsBetweenRequests = oldValue

    def t1est_async_order1(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200:3', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.5, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)

    def t1est_async_order2(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200:3', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.7, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)

    def t1est_async_noorder1(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200:3', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ], maintainOrder = False):
            responses.add(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.1, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' }, responses)

    def t1est_async_noorder2(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200:3', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ], maintainOrder = False):
            responses.add(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.5, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' }, responses)

    def t1est_empty(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([]):
            self.fail()

        self.assertAlmostEqual(time() - start, 0, delta = 0.04)

    def t1est_sync_exception1(self):
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:450')
            self.fail()
        except HTTPError as err:
            self.assertEqual(err.msg, 'Test')
            self.assertEqual(err.code, 450)

        self.assertAlmostEqual(time() - start, 5.2, delta = 0.04)

    def t1est_sync_exception2(self):
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:640')
            self.fail()
        except Exception as err:
            self.assertEqual(str(err), '[640] Test')

        self.assertAlmostEqual(time() - start, 0.4, delta = 0.04)

    def t1est_sync_noraise_exception1(self):
        start = time()
        r1 = self.noRaise.one('http://cat-videos.net/1/Test:450')
        self.assertEqual(r1.reason, 'Test')
        self.assertEqual(r1.status_code, 450)
        self.assertAlmostEqual(time() - start, 5.2, delta = 0.04)

    def t1est_sync_noraise_exception2(self):
        start = time()
        try:
            self.noRaise.one('http://cat-videos.net/1/Test:640')
            self.fail()
        except Exception as err:
            self.assertEqual(str(err), '[640] Test')

        self.assertAlmostEqual(time() - start, 0.4, delta = 0.04)

    def t1est_sync_notrequest(self):
        start = time()
        try:
            self.default.one(123)
            self.fail()
        except TypeError as err:
            pass
        self.assertAlmostEqual(time() - start, 0, delta = 0.04)

    def t1est_sync_lenient1(self):
        oldValue = self.default.retryStrategy
        self.default.retryStrategy = Lenient()
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:550')
            self.fail()
        except HTTPError as err:
            self.assertEqual(err.msg, 'Test')
            self.assertEqual(err.code, 550)

        self.assertAlmostEqual(time() - start, 242, delta = 0.04)
        self.default.retryStrategy = oldValue

    def t1est_sync_lenient2(self):
        oldValue = self.default.retryStrategy
        self.default.retryStrategy = Lenient()
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:650')
            self.fail()
        except Exception as err:
            self.assertEqual(str(err), '[650] Test')

        self.assertAlmostEqual(time() - start, 60.8, delta = 0.04)
        self.default.retryStrategy = oldValue

    def t1est_sync_backoff1(self):
        oldValue = self.default.retryStrategy
        self.default.retryStrategy = Backoff()
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:560')
            self.fail()
        except HTTPError as err:
            self.assertEqual(err.msg, 'Test')
            self.assertEqual(err.code, 560)

        self.assertAlmostEqual(time() - start, 247.9, delta = 0.04)
        self.default.retryStrategy = oldValue

    def t1est_sync_backoff2(self):
        oldValue = self.default.retryStrategy
        self.default.retryStrategy = Backoff()
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:660')
            self.fail()
        except Exception as err:
            self.assertEqual(str(err), '[660] Test')

        self.assertAlmostEqual(time() - start, 10.8, delta = 0.04)
        self.default.retryStrategy = oldValue

    def t1est_swarm_in_swarm_order1(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.default.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def t1est_swarm_in_swarm_order2(self):
        responses = []
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.highConcurrency.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def t1est_swarm_in_swarm_order3(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.default.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200:0.6' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.6, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def t1est_swarm_in_swarm_order4(self):
        responses = []
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.highConcurrency.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200:0.6' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.4, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def t1est_swarm_in_swarm_noorder1(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.default.swarm([ r1.url + '/A/OK:200:1', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ], maintainOrder = False):
                responses.add(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.7, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' }, responses)

    def t1est_swarm_in_swarm_noorder2(self):
        responses = set()
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.highConcurrency.swarm([ r1.url + '/A/OK:200:1', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ], maintainOrder = False):
                responses.add(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.6, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' }, responses)

class TestGC(TestCase):
    def test_all_swarm_get_executed(self):
        requests = Requests()

        # Monkey patch the actual send to make testing timings easier
        def fake_send(self, request):
            print request.url

        requests.session.send = MethodType(fake_send, requests.session, Session)

        requests.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ])

        sleep(0.2)

if __name__ == '__main__':
    main()