# -*- coding: utf-8 -*-

"""Simple-requests allows you to get the performance benefit of asynchronous
requests, without needing to use any asynchronous coding paradigms.

See http://pythonhosted.org/simple-requests for HTML documentation.

Goal
----

The goal of this library is to allow you to get the performance benefit of asynchronous requests, without needing to use any asynchronous coding paradigms.  It is built on `gevent <https://github.com/surfly/gevent>`_ and `requests <https://github.com/kennethreitz/requests>`_.

If you like getting your hands dirty, the :class:`gevent.Pool` and :class:`requests.Session` that drives the main object is readily available for you to tinker with as much as you'd like.

Features
--------

There is also some added functionality not available out-of-the-box from the base libraries:

* Request thresholding
* Automatic retry on failure, with three different retry strategies included that focus on different applications (big server scrape, small server scrape, API)
* Lazy loading and minimal object caching to keep the memory footprint down

Usage
-----
.. code-block:: python

    from simple_requests import Requests

    # Creates a session and thread pool
    requests = Requests()

    # Sends one simple request; the response is returned synchronously.
    login_response = requests.one('http://cat-videos.net/login?user=fanatic&password=c4tl0v3r')

    # Cookies are maintained in this instance of Requests, so subsequent requests
    # will still be logged-in.
    profile_urls = [
        'http://cat-videos.net/profile/mookie',
        'http://cat-videos.net/profile/kenneth',
        'http://cat-videos.net/profile/itchy' ]

    # Asynchronously send all the requests for profile pages
    for profile_response in requests.swarm(profile_urls):

        # Asynchronously send requests for each link found on the profile pages
        # These requests take precedence over those in the outer loop to minimize overall waiting
        # Order doesn't matter this time either, so turn that off for a performance gain
        for friends_response in requests.swarm(profile_response.links, maintainOrder = False):

            # Do something intelligent with the responses, like using
            # regex to parse the HTML (see http://stackoverflow.com/a/1732454)
            friends_response.html

API
---
"""

# This needs to be first
import gevent.monkey; gevent.monkey.patch_all(thread=False, select=False)

from atexit import register
from bisect import bisect_right
from collections import OrderedDict
from time import time
from urllib2 import HTTPError as libHTTPError
from weakref import WeakSet

from gevent import GreenletExit, killall, sleep, spawn
from gevent.event import Event
from gevent.pool import Pool

from requests import PreparedRequest, Request, Response, Session

__all__ = ( 'Requests', 'ResponsePreprocessor', 'Strict', 'Lenient', 'Backoff', 'HTTPError' )


class HTTPError(libHTTPError):
    """Encapsulates server errors (status codes in the 400s and 500s).

    .. attribute:: code

        Status code for this error.

    .. attribute:: msg

        The reason (associated with the status code).

    .. attribute:: response

        The instance of :class:`requests.Response` which triggered the error.
    """
    def __init__(self, response):
        super(HTTPError, self).__init__(response.url, response.status_code, response.reason, response.headers, response.raw)
        self.response = response


class ResponsePreprocessor(object):
    """Default implementation of how responses are preprocessed.

    By default, successful responses are returned and errors are raised.
    Whatever is returned by :meth:`success` and :meth:`error` is what gets
    returned by :meth:`Responses.one` and the iterator of :meth:`Responses.swarm`.

    There are several reasons you may want to override the default
    implementation:

    * Don't raise an exception on server errors
    * Add a side-effect, such as writing all responses to an archive file
    * Responses in your application must always be pre-processed in a
      specific way
    * More...
    """
    def success(self, response):
        return response

    def error(self, exception):
        raise exception


class RetryStrategy(object):
    """The base implementation, which doesn't retry at all."""
    def retry(self, request, exception, numTries):
        return -1

class Strict(RetryStrategy):
    """A good default :class:`RetryStrategy`.

    Retries up to two times, with 2 seconds between each attempt.

    Non-server errors are not retried.
    """
    def retry(self, request, exception, numTries):
        if isinstance(exception, HTTPError) and numTries < 3:
            return 2
        else:
            return -1

class Lenient(RetryStrategy):
    """A :class:`RetryStrategy` designed for very small servers.

    Small servers are expected to go down every now and then, so this
    strategy retries requests up to 4 times, with a full minute between each
    attempt.

    Non-server errors are retried only once after 60 seconds.
    """
    def retry(self, request, exception, numTries):
        if numTries == 1 or (isinstance(exception, HTTPError) and numTries < 5):
            return 60
        else:
            return -1

class Backoff(RetryStrategy):
    """A :class:`RetryStrategy` designed for APIs.

    Since APIs are *expected* to work, this implementation retries many times,
    with an exponentially-increasing time in between each request (capped at
    60 seconds).

    Non-server errors are retried only once after 10 seconds.
    """
    def retry(self, request, exception, numTries):
        if isinstance(exception, HTTPError):
            # Returns 0.5, 1, 2, 4, 8, 16, 32, 60, 60, 60, -1
            if numTries < 11:
                return min(2**(numTries-2), 60)
            else:
                return -1
        else:
            if numTries == 1:
                return 10
            else:
                return -1


class _ResponseIterator(object):
    def __init__(self, maintainOrder, preprocessor):
        self._currentIndex = 0 if maintainOrder else None
        self._preprocessor = preprocessor
        self._responseAdded = Event()
        self._responses = OrderedDict()
        self._inflight = 0
        self._done = False

    def __iter__(self):
        return self

    def _add(self, response, exception, requestIndex):
        if self._responses is not None:
            self._responses[requestIndex] = ( response, exception )
        self._inflight -= 1
        self._responseAdded.set()

    def next(self):
        while True:
            if self._inflight == 0 and self._done and (self._responses is None or len(self._responses) == 0):
                raise StopIteration

            found = False
            if self._responses is not None and len(self._responses) > 0:
                if self._currentIndex is None:
                    found = True
                    response, exception = self._responses.popitem(last = False)[1]
                else:
                    if self._currentIndex in self._responses:
                        found = True
                        response, exception = self._responses.pop(self._currentIndex)
                        self._currentIndex += 1

            if found:
                if exception is None:
                    return self._preprocessor.success(response)
                else:
                    return self._preprocessor.error(exception)
            else:
                self._responseAdded.clear()
                self._responseAdded.wait()


class _RequestQueue(object):
    def __init__(self):
        self.queue = [] # requestIterator, nextRequest, responseIterator, group, requestIndex
        self.group = 0

    def add(self, requestIterator, responseIterator):
        try:
            next = requestIterator.next()
            self.queue.insert(0, [ requestIterator, next, responseIterator, self.group, 0 ])
            self.group += 1
        except StopIteration:
            responseIterator._done = True

    def getLatestGroup(self):
        if len(self.queue):
            return self.queue[0][3]
        else:
            return None

    def pop(self):
        """Assumes getLatestGroup was called immediately before pop and returned not-None, on the same thread, with no slices in between"""
        status = self.queue[0]
        ret = tuple(status[1:])
        status[2]._inflight += 1
        try:
            status[1] = status[0].next()
            status[4] += 1
        except StopIteration:
            self.queue.pop(0)
            status[2]._done = True
        return ret

    def stop(self):
        while len(self.queue):
            responseIterator = self.queue.pop(0)[2]
            responseIterator._done = True
            if responseIterator._inflight == 0:
                responseIterator._responseAdded.set()


class _RetryQueue(object):
    def __init__(self):
        self.timelist = []
        self.sortedqueue = [] # request, responseIterator, group, requestIndex, numTries, nextAttempt

    def add(self, request, responseIterator, group, requestIndex, numTries, wait):
        nextAttempt = time() + wait
        i = bisect_right(self.timelist, nextAttempt)
        self.timelist.insert(i, nextAttempt)
        self.sortedqueue.insert(i, ( request, responseIterator, group, requestIndex, numTries, nextAttempt ))

    def getLatestGroup(self):
        now = time()
        self.maxStatus = None
        index = 0
        for s in self.sortedqueue[:bisect_right(self.timelist, now + 0.001)]: # Add a small epsilon to handle floating-point shenanigans
            if self.maxStatus is None or self.maxStatus[2] < s[2]:
                self.maxStatus = s
                self.maxIndex = index
            index += 1
        return None if self.maxStatus is None else self.maxStatus[2]

    def getMinWaitTime(self):
        if len(self.timelist):
            return self.timelist[0] - time()
        else:
            return None

    def pop(self):
        """Assumes getLatestGroup was called immediately before pop and returned not-None, on the same thread, with no slices in between"""
        del self.timelist[self.maxIndex]
        del self.sortedqueue[self.maxIndex]
        return self.maxStatus[:5]

    def stop(self):
        while len(self.sortedqueue):
            responseIterator = self.sortedqueue.pop(0)[1]
            responseIterator._inflight -= 1
            if responseIterator._inflight == 0:
                responseIterator._responseAdded.set()
        self.timelist = []


_inFlight = WeakSet()
register(killall, _inFlight)


class Requests(object):
    """A session of requests.

    Creates a session and a thread pool. The intention is to have one instance
    per server that you're hitting at the same time.

    :param concurrent: (optional) The maximum number of concurrent requests
                       allowed for this instance.
    :param minSecondsBetweenRequests: (optional) Every request is guaranteed to
                                      be separated by at least this many
                                      seconds.
    :param retryStrategy: (optional) An instance of :class:`RetryStrategy` (or
                          subclass). Allows you to define if and how a request
                          should be retried on failure. The default
                          implementation (:class:`Strict`) retries failed
                          requests twice, for server errors only, with at least
                          2 seconds between each subsequent request. Two other
                          implementations are included: :class:`Lenient` (good
                          for really small servers, perhaps hosted out of
                          somebody's home), and :class:`Backoff` (good for
                          APIs).
    :param responsePreprocessor: (optional) An instance of
                                 :class:`ResponsePreprocessor` (or subclass).
                                 Useful if you need to override the default
                                 handling of successful responses and/or failed
                                 responses.
    .. attribute:: session

        An instance of :class:`requests.Session` that manages things like
        maintaining cookies between requests.

    .. attribute:: pool

        An instance of :class:`gevent.Pool` that manages things like
        maintaining the number of concurrent requests.  Changes to this object
        should be done before any requests are sent.
    """
    def __init__(self, concurrent = 2, minSecondsBetweenRequests = 0.15, retryStrategy = Strict(), responsePreprocessor = ResponsePreprocessor()):
        if not isinstance(retryStrategy, RetryStrategy):
            raise TypeError('retryStrategy must be an instance of RetryStrategy, not %s' % type(retryStrategy))

        if not isinstance(responsePreprocessor, ResponsePreprocessor):
            raise TypeError('responsePreprocessor must be an instance of ResponsePreprocessor, not %s' % type(responsePreprocessor))

        self.session = Session()
        self.pool = Pool(concurrent)
        self.minSecondsBetweenRequests = minSecondsBetweenRequests
        self.retryStrategy = retryStrategy
        self.responsePreprocessor = responsePreprocessor

        self._requestGroups = 0
        self._requestAdded = Event()
        self._requestQueue = _RequestQueue()
        self._retryQueue = _RetryQueue()

        _inFlight.add(spawn(self._run))

    def _run(self):
        done = False

        while True:
            try:
                self.pool.wait_available()
                reqGroup = self._requestQueue.getLatestGroup()
                retryGroup = self._retryQueue.getLatestGroup()

                if reqGroup is None and retryGroup is None:
                    if done:
                        break
                    else:
                        self._requestAdded.clear()
                        self._requestAdded.wait(self._retryQueue.getMinWaitTime())
                
                else:
                    builtrequest = None
                    if retryGroup is None or (reqGroup is not None and reqGroup > retryGroup):
                        request, responseIterator, group, requestIndex = self._requestQueue.pop()
                        numTries = 0
                        try:
                            builtrequest = self._build(request)
                        except Exception as ex:
                            self._response(( request, None, responseIterator, group, requestIndex, numTries, ex ))
                            request = None
                    else:
                        builtrequest, responseIterator, group, requestIndex, numTries = self._retryQueue.pop()

                    if isinstance(builtrequest, PreparedRequest):
                        self.pool.spawn(self._execute, builtrequest, responseIterator, group, requestIndex, numTries).rawlink(self._response)
                        if self.minSecondsBetweenRequests > 0:
                            sleep(self.minSecondsBetweenRequests)

                    elif builtrequest:
                        self._response(( request, builtrequest, responseIterator, group, requestIndex, numTries, None ))

            except GreenletExit:
                done = True

    def _build(self, request):
        if isinstance(request, basestring):
            request = Request(method = 'GET', url = request)
        if isinstance(request, Request):
            request = self.session.prepare_request(request)
        if not isinstance(request, PreparedRequest):
            raise TypeError('Request must be an instance of: str (or unicode), Request, PreparedRequest, not %s.' % type(request))
        return request

    def _execute(self, request, responseIterator, group, requestIndex, numTries):
        try:
            response = self.session.send(request)

            if response.status_code >= 400:
                return request, response, responseIterator, group, requestIndex, numTries, HTTPError(response)
            else:
                return request, response, responseIterator, group, requestIndex, numTries, None
        except (Exception, GreenletExit) as ex:
            return request, None, responseIterator, group, requestIndex, numTries, ex
    
    def _response(self, status):
        if isinstance(status, tuple):
            request, response, responseIterator, group, requestIndex, numTries, exception = status
            stopped = False
        else:
            request, response, responseIterator, group, requestIndex, numTries, exception = status.value
            stopped = hasattr(status, 'stopped')
        numTries += 1

        if exception is None:
            responseIterator._add(response, None, requestIndex)
        elif isinstance(exception, GreenletExit):
            # Execution was killed
            responseIterator._inflight -= 1
            if responseIterator._inflight == 0:
                responseIterator._responseAdded.set()
        else:
            if stopped:
                # A stop was sent, so don't add to the retry queue regardless of strategy
                responseIterator._add(response, exception, requestIndex)
            else:
                wait = self.retryStrategy.retry(request, exception, numTries)
                if wait >= 0:
                    self._retryQueue.add(request, responseIterator, group, requestIndex, numTries, wait)
                    self._requestAdded.set()
                else:
                    responseIterator._add(response, exception, requestIndex)

    def _add(self, requestIterator, maintainOrder, responsePreprocessor):
        if responsePreprocessor is not None and not isinstance(responsePreprocessor, ResponsePreprocessor):
            raise TypeError('responsePreprocessor must be an instance of ResponsePreprocessor, not %s' % type(responsePreprocessor))
        responseIterator = _ResponseIterator(maintainOrder, responsePreprocessor or self.responsePreprocessor)
        self._requestQueue.add(requestIterator, responseIterator)
        self._requestAdded.set()
        return responseIterator

    def one(self, request, responsePreprocessor = None):
        """Execute one request synchronously.

        Since this request is synchronous, it takes precedence over any other
        :meth:`swarm` calls which may still be processing.

        :param request: A :class:`str`, :class:`requests.Request`, or
                        :class:`requests.PreparedRequest`.  :class:`str`
                        (or any other :class:`basestring`) will be executed as
                        an HTTP ``GET``.
        :param responsePreprocessor: (optional) Override the default
                                     preprocessor for this request only.
        :returns: A :class:`requests.Response`.
        """
        return self._add([ request ].__iter__(), False, responsePreprocessor).next()

    def swarm(self, iterable, maintainOrder = True, responsePreprocessor = None):
        """Execute each request asynchronously.

        Subsequent calls to :meth:`swarm` or :meth:`one` on the same
        :class:`Requests` instance will be prioritized *over* earlier calls.
        This is generally aligned with how responses are processed (one
        response is inspected, which leads to more requests whose responses
        are inspected... etc.)

        This method will try hard to finish executing all requests, even if the
        iterator has fallen out of scope, or an exception was raised, or even
        if the execution of the main module is finished.  Use the :meth:`stop`
        method to cancel any pending requests and/or kill executing requests. 

        :param iterable: A generator, list, tuple, dictionary, or any other
                         iterable object containing any combination of
                         :class:`str`, :class:`requests.Request`,
                         :class:`requests.PreparedRequest`.  :class:`str`
                         (or any other :class:`basestring`) will be executed
                         as an HTTP ``GET``.
        :param maintainOrder: (optional) By default, the returned responses are
                              guaranteed to be in the same order as the
                              requests.  If this is not important to you, set
                              this to False for a performance gain.
        :param responsePreprocessor: (optional) Override the default
                                     preprocessor for these requests only.
        :returns: A :class:`ResponseIterator` that may be iterated over to get a
                  :class:`requests.Response` for each request.
        """
        return self._add(iterable.__iter__(), maintainOrder, responsePreprocessor)

    def stop(self, killExecuting = True):
        """Stop the execution of requests early.

        The :meth:`swarm` method will try hard to finish executing all requests,
        even if the iterator has fallen out of scope, or an exception was
        raised, or even if the execution of the main module is finished.

        Use this method to cancel all pending requests.

        :param killExecuting: (optional) In addition to canceling pending
                              requests, kill any currently-executing requests
                              so that the response will not be returned. While
                              this has the benefit of guaranteeing that there
                              will be no more activity once the method returns,
                              it means that it is undeterminable whether any
                              current requests succeeded, failed, or had any
                              server side-effects.
        """
        self._requestQueue.stop()
        self._retryQueue.stop()
        if killExecuting:
            self.pool.kill()
        else:
            for greenlet in self.pool.greenlets:
                setattr(greenlet, 'stopped', True)
