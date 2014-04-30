#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test suite for simple-requests

Please note that many of the tests check that the timing of a certain number
of synthetic requests fall within a very tight time range (0.04 seconds).

Slow computers, or running the tests in the background may fail these tests.
"""

from gevent import sleep
from random import random
from re import compile
from requests import Response, Session
from time import time
from types import MethodType
from unittest import main, TestCase

from simple_requests import *

# Pre-load some things that are normally done lazily, to avoid messing up our times.
#'abc'.encode('idna').decode('utf-8')
#from netrc import netrc, NetrcParseError
#print '4e[' + str(hex(id(getcurrent()))) + '] ' + str(time())

class NoRaiseServerError(ResponsePreprocessor):
    def error(self, bundle):
        if isinstance(bundle.exception, HTTPError):
            return bundle.ret()
        else:
            raise bundle.exception

class Test1Logic(TestCase):
    def setUp(self):
        self.default = Requests()
        self.highConcurrency = Requests(concurrent = 5)
        self.noRaise = Requests(responsePreprocessor = NoRaiseServerError())
        self.defaultSendTime = defaultSendTime = 0.4
        self.defaultRetryWait = 2

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
                wait = defaultSendTime

            sleep(wait)

            if response.status_code >= 600:
                # Special case for testing exception handling
                raise Exception('[%d] %s' % ( response.status_code, response.reason ))

            return response
        self.default.session.send = MethodType(fake_send, self.default.session, Session)
        self.highConcurrency.session.send = MethodType(fake_send, self.highConcurrency.session, Session)
        self.noRaise.session.send = MethodType(fake_send, self.noRaise.session, Session)

        # The first request always suffers through various init times of lazily-loaded objects;
        #  make a throw-away one here to avoid affecting the tests
        self.default.one('http://cat-videos.net/1/OK:200').url

    def test_sync(self):
        start = time()

        self.assertEqual(self.default.one('http://cat-videos.net/1/OK:200').url, 'http://cat-videos.net/1')
        self.assertEqual(self.default.one('http://cat-videos.net/2/OK:200').url, 'http://cat-videos.net/2')
        self.assertEqual(self.default.one('http://cat-videos.net/3/OK:200').url, 'http://cat-videos.net/3')
        self.assertEqual(self.default.one('http://cat-videos.net/4/OK:200').url, 'http://cat-videos.net/4')
        self.assertEqual(self.default.one('http://cat-videos.net/5/OK:200').url, 'http://cat-videos.net/5')

        self.assertAlmostEqual(time() - start, self.defaultSendTime * 5, delta = 0.04)

    def test_async(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, self.defaultSendTime * 3, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)

    def test_async_high_mintime(self):
        responses = []
        oldValue = self.default.minSecondsBetweenRequests
        self.default.minSecondsBetweenRequests = 0.25
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, self.default.minSecondsBetweenRequests * 4 + self.defaultSendTime, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)
        self.default.minSecondsBetweenRequests = oldValue

    def test_async_high_concurrency(self):
        responses = []
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200', 'http://cat-videos.net/6/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, self.highConcurrency.minSecondsBetweenRequests * 5 + self.defaultSendTime, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5', 'http://cat-videos.net/6' ], responses)

    def test_async_low_mintime1(self):
        responses = []
        oldValue = self.highConcurrency.minSecondsBetweenRequests
        self.highConcurrency.minSecondsBetweenRequests = 0.05
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, self.highConcurrency.minSecondsBetweenRequests * 4 + self.defaultSendTime, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)
        self.highConcurrency.minSecondsBetweenRequests = oldValue

    def test_async_low_mintime2(self):
        responses = []
        oldValue = self.highConcurrency.minSecondsBetweenRequests
        self.highConcurrency.minSecondsBetweenRequests = 0.05
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200', 'http://cat-videos.net/6/OK:200' ]):
            responses.append(r1.url)

        self.assertAlmostEqual(time() - start, 0.8, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5', 'http://cat-videos.net/6' ], responses)
        self.highConcurrency.minSecondsBetweenRequests = oldValue

    def test_async_order1(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200:3', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.5, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)

    def test_async_order2(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200:3', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ]):
            responses.append(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.7, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' ], responses)

    def test_async_noorder1(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200:3', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ], maintainOrder = False):
            responses.add(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.1, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' }, responses)

    def test_async_noorder2(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200:3', 'http://cat-videos.net/4/OK:200', 'http://cat-videos.net/5/OK:200' ], maintainOrder = False):
            responses.add(r1.url)
            sleep(0.1)

        self.assertAlmostEqual(time() - start, 3.5, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4', 'http://cat-videos.net/5' }, responses)

    def test_empty(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([]):
            self.fail()

        self.assertAlmostEqual(time() - start, 0, delta = 0.04)

    def test_sync_exception1(self):
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:450')
            self.fail()
        except HTTPError as err:
            self.assertEqual(err.msg, 'Test')
            self.assertEqual(err.code, 450)

        self.assertAlmostEqual(time() - start, 5.2, delta = 0.04)

    def test_sync_exception2(self):
        start = time()
        try:
            self.default.one('http://cat-videos.net/1/Test:640')
            self.fail()
        except Exception as err:
            self.assertEqual(str(err), '[640] Test')

        self.assertAlmostEqual(time() - start, 0.4, delta = 0.04)

    def test_sync_noraise_exception1(self):
        start = time()
        r1 = self.noRaise.one('http://cat-videos.net/1/Test:450')
        self.assertEqual(r1.reason, 'Test')
        self.assertEqual(r1.status_code, 450)
        self.assertAlmostEqual(time() - start, 5.2, delta = 0.04)

    def test_sync_noraise_exception2(self):
        start = time()
        try:
            self.noRaise.one('http://cat-videos.net/1/Test:640')
            self.fail()
        except Exception as err:
            self.assertEqual(str(err), '[640] Test')

        self.assertAlmostEqual(time() - start, 0.4, delta = 0.04)

    def test_sync_notrequest(self):
        start = time()
        try:
            self.default.one(123)
            self.fail()
        except TypeError as err:
            pass
        self.assertAlmostEqual(time() - start, 0, delta = 0.04)

    def test_sync_lenient1(self):
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

    def test_sync_lenient2(self):
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

    def test_sync_backoff1(self):
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

    def test_sync_backoff2(self):
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

    def test_swarm_in_swarm_order1(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.default.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def test_swarm_in_swarm_order2(self):
        responses = []
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.highConcurrency.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def test_swarm_in_swarm_order3(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.default.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200:0.6' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.6, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def test_swarm_in_swarm_order4(self):
        responses = []
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.highConcurrency.swarm([ r1.url + '/A/OK:200', r1.url + '/B/OK:200', r1.url + '/C/OK:200:0.6' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.4, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' ], responses)

    def test_big_swarm_in_swarm_order(self):
        responses = []
        oldValue = self.default.minSecondsBetweenRequests
        self.default.minSecondsBetweenRequests = 0
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200:3', 'http://cat-videos.net/2/OK:200:1', 'http://cat-videos.net/3/OK:200:3', 'http://cat-videos.net/4/OK:200:5' ]):
            r2 = self.default.one(r1.url + '/X/OK:200:1')
            for r3 in self.default.swarm([ r2.url + '/A/OK:200:2', r2.url + '/B/OK:200:1', r2.url + '/C/OK:200:1' ]):
                responses.append(r3.url[22:])
        self.assertAlmostEqual(time() - start, 17, delta = 0.1)
        self.assertEqual([ '1/X/A', '1/X/B', '1/X/C', '2/X/A', '2/X/B', '2/X/C', '3/X/A', '3/X/B', '3/X/C', '4/X/A', '4/X/B', '4/X/C' ], responses)    
        self.default.minSecondsBetweenRequests = oldValue

    def test_big_swarm_in_swarm_noorder(self):
        responses = set()
        oldValue = self.default.minSecondsBetweenRequests
        self.default.minSecondsBetweenRequests = 0
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200:3', 'http://cat-videos.net/2/OK:200:1', 'http://cat-videos.net/3/OK:200:3', 'http://cat-videos.net/4/OK:200:5' ], maintainOrder = False):
            r2 = self.default.one(r1.url + '/X/OK:200:1')
            for r3 in self.default.swarm([ r2.url + '/A/OK:200:2', r2.url + '/B/OK:200:1', r2.url + '/C/OK:200:1' ], maintainOrder = False):
                responses.add(r3.url[22:])
        self.assertAlmostEqual(time() - start, 17, delta = 0.1)
        self.assertEqual({ '1/X/A', '1/X/B', '1/X/C', '2/X/A', '2/X/B', '2/X/C', '3/X/A', '3/X/B', '3/X/C', '4/X/A', '4/X/B', '4/X/C' }, responses)    
        self.default.minSecondsBetweenRequests = oldValue

    def test_swarm_in_swarm_noorder1(self):
        responses = set()
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.default.swarm([ r1.url + '/A/OK:200:1', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ], maintainOrder = False):
                responses.add(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.7, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' }, responses)

    def test_swarm_in_swarm_noorder2(self):
        responses = set()
        start = time()
        for r1 in self.highConcurrency.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200' ]):
            for r2 in self.highConcurrency.swarm([ r1.url + '/A/OK:200:1', r1.url + '/B/OK:200', r1.url + '/C/OK:200' ], maintainOrder = False):
                responses.add(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 2.6, delta = 0.04)
        self.assertEqual({ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/C', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/C' }, responses)

    def test_swarm_in_swarm_order_exception(self):
        responses = []
        start = time()
        for r1 in self.noRaise.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200:1', 'http://cat-videos.net/3/OK:200' ]):
            for r2 in self.noRaise.swarm([ r1.url + '/A/Gone:410', r1.url + '/B/OK:200' ]):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 16.6, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/A', 'http://cat-videos.net/1/B', 'http://cat-videos.net/2/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/3/A', 'http://cat-videos.net/3/B' ], responses)

    def test_swarm_in_swarm_noorder_exception(self):
        responses = []
        start = time()
        for r1 in self.noRaise.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200:1', 'http://cat-videos.net/3/OK:200' ]):
            for r2 in self.noRaise.swarm([ r1.url + '/A/Gone:410', r1.url + '/B/OK:200' ], maintainOrder = False):
                responses.append(r2.url)
                sleep(0.1)

        self.assertAlmostEqual(time() - start, 16.3, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1/B', 'http://cat-videos.net/1/A', 'http://cat-videos.net/2/B', 'http://cat-videos.net/2/A', 'http://cat-videos.net/3/B', 'http://cat-videos.net/3/A' ], responses)

    def test_swarm_stop1(self):
        responses = []
        start = time()
        for r1 in self.noRaise.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200' ]):
            responses.append(r1.url)
            # The third request is sent before this stop is issued.  This is because the pool wait is released (when _execute completes) *before* the iterator event is fired (which happens in _response)
            self.noRaise.stop(killExecuting = False)

        self.assertAlmostEqual(time() - start, self.defaultSendTime * 2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3' ], responses)

    def test_swarm_stop2(self):
        responses = []
        start = time()
        for r1 in self.noRaise.swarm([ 'http://cat-videos.net/1/Test:418', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200' ], maintainOrder = False):
            responses.append(r1.url)
            self.noRaise.stop(killExecuting = False)

        self.assertAlmostEqual(time() - start, self.defaultSendTime * 2 + self.noRaise.minSecondsBetweenRequests, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4' ], responses)

    def test_swarm_stop3(self):
        responses = []
        start = time()
        for r1 in self.noRaise.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/Test:418', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200' ]):
            responses.append(r1.url)
            self.noRaise.stop(killExecuting = False)

        self.assertAlmostEqual(time() - start, self.defaultSendTime * 2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3' ], responses)

    def test_swarm_stop4(self):
        responses = []
        start = time()
        for r1 in self.noRaise.swarm([ 'http://cat-videos.net/1/Test:418', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200', 'http://cat-videos.net/4/OK:200' ]):
            responses.append(r1.url)
            self.noRaise.stop(killExecuting = False)

        self.assertAlmostEqual(time() - start, self.defaultSendTime * 3 + self.defaultRetryWait * 2, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1', 'http://cat-videos.net/2', 'http://cat-videos.net/3', 'http://cat-videos.net/4' ], responses)

    def test_swarm_stop5(self):
        start = time()
        it =  self.noRaise.swarm([ 'http://cat-videos.net/1/Test:418' ])
        sleep(0.1)
        self.noRaise.stop(killExecuting = False)
        response = it.next()
        self.assertAlmostEqual(time() - start, self.defaultSendTime, delta = 0.04)
        self.assertEqual('http://cat-videos.net/1', response.url)

    def test_swarm_stop_and_kill1(self):
        responses = []
        start = time()
        for r1 in self.default.swarm([ 'http://cat-videos.net/1/OK:200', 'http://cat-videos.net/2/OK:200', 'http://cat-videos.net/3/OK:200' ]):
            responses.append(r1.url)
            self.default.stop()

        self.assertAlmostEqual(time() - start, 0.4, delta = 0.04)
        self.assertEqual([ 'http://cat-videos.net/1' ], responses)

    def test_swarm_stop_and_kill2(self):
        start = time()
        it =  self.noRaise.swarm([ 'http://cat-videos.net/1/Test:418' ])
        sleep(0.1)
        self.noRaise.stop()
        try:
            it.next()
            self.fail()
        except StopIteration:
            self.assertAlmostEqual(time() - start, 0.1, delta = 0.04)

    def test_custom_preprocessor(self):
        class CustomPreprocessor(ResponsePreprocessor):
            def success(self, bundle):
                bundle.response.url += '!'
                return bundle.ret()

        start = time()
        self.assertEqual(self.default.one('http://cat-videos.net/1/OK:200', responsePreprocessor = CustomPreprocessor()).url, 'http://cat-videos.net/1!')
        self.assertAlmostEqual(time() - start, 0.4, delta = 0.04)

    def test_each(self):
        class Obj(object):
            def __init__(self, data, request):
                self.data = data
                self.request = request

        responses = []
        start = time()
        for r1, obj in self.noRaise.each([ Obj('AAA', 'http://cat-videos.net/1/Test:416'), Obj('BBB', 'http://cat-videos.net/2/OK:200') ]):
            responses.append(( r1.url, r1.status_code, obj.data ))

        self.assertAlmostEqual(time() - start, 5.2, delta = 0.04)
        self.assertEqual([ ( 'http://cat-videos.net/2', 200, 'BBB' ), ( 'http://cat-videos.net/1', 416, 'AAA' ) ], responses)

    def test_each_custom_map(self):
        class Obj(object):
            def __init__(self, data, status):
                self.data = data
                self.status = status

        class Mapper(object):
            def __init__(self):
                self.count = 0

            def torequest(self, i):
                self.count += 1
                return 'http://cat-videos.net/%d/%s' % ( self.count, i.status)

        responses = []
        start = time()
        for r1, obj in self.noRaise.each([ Obj('XXX', 'OK:200:1'), Obj('YYY', 'OK:200') ], mapToRequest = Mapper().torequest):
            responses.append(( r1.url, r1.status_code, obj.data ))

        self.assertAlmostEqual(time() - start, 1, delta = 0.04)
        self.assertEqual([ ( 'http://cat-videos.net/2', 200, 'YYY' ), ( 'http://cat-videos.net/1', 200, 'XXX' ) ], responses)


class Test2RealRequests(TestCase):
    def setUp(self):
        self.requests = Requests(concurrent = 4)

    def url(self, delay, key):
        return 'http://httpbin.org/delay/%s?key=%s' % ( delay, key )

    def key(self, response):
        return response.json()['args']['key']

    def test_big_swarm_in_swarm_order(self):
        responses = []
        start = time()
        for r1 in self.requests.swarm([ self.url(3, '1'), self.url(1, '2'), self.url(3, '3'), self.url(5, '4') ]):
            r2 = self.requests.one(self.url(1, self.key(r1) + 'x'))
            for r3 in self.requests.swarm([ self.url(2, self.key(r2) + 'A'), self.url(1, self.key(r2) + 'B'), self.url(1, self.key(r2) + 'C') ]):
                responses.append(self.key(r3))
        self.assertLess(time() - start, 30) # Non-async has a minimum bound of 32 seconds
        self.assertEqual([ '1xA', '1xB', '1xC', '2xA', '2xB', '2xC', '3xA', '3xB', '3xC', '4xA', '4xB', '4xC' ], responses)


class Test3InFlight(TestCase):
    def test_all_swarm_get_executed(self):
        requests = Requests()

        # Monkey patch the actual send to print the url to console, so we can see if it worked
        def fake_send(self, request):
            print request.url

        requests.session.send = MethodType(fake_send, requests.session, Session)

        print '\n*** This is an eyeball test: make sure all 5 urls are printed to the console ***'
        requests.swarm([ 'http://cat-videos.net/1-of-5', 'http://cat-videos.net/2-of-5', 'http://cat-videos.net/3-of-5', 'http://cat-videos.net/4-of-5', 'http://cat-videos.net/5-of-5' ])

if __name__ == '__main__':
    main(verbosity = 2)
