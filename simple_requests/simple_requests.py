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
* Checks with smart defaults to avoid killing your system (e.g. opening too many connections at once) or the remote server (e.g. making too many requests per second)

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

from atexit import register
from bisect import bisect_right
from collections import OrderedDict
from sys import exc_info
from time import time
from weakref import WeakSet

from gevent import Greenlet, GreenletExit, killall, sleep
from gevent.event import Event
from gevent.pool import Pool

from requests import PreparedRequest, Request, Response, Session
from requests.adapters import HTTPAdapter

from compat import HTTPError
from strategy import RetryStrategy, Strict


class ResponsePreprocessor(object):
    """Default implementation of how responses are preprocessed.

    By default, successful responses are returned and errors are raised.
    Whatever is returned by :meth:`success` and :meth:`error` is what gets
    returned by :meth:`Responses.one` and the iterator of :meth:`Responses.swarm`.

    There are several reasons you may want to override the default
    implementation:

    * Don't raise an exception on HTTP errors
    * Add a side-effect, such as writing all responses to an archive file
    * Responses in your application must always be pre-processed in a
      specific way
    * More...
    """
    def success(self, bundle):
        return bundle.ret()

    def error(self, bundle):
        raise type(bundle.exception), bundle.exception, bundle.traceback


class Bundle(object):
    def __init__(self, request):
        self.request = request
        self.response = None
        self.exception = None
        self.traceback = None
        self.obj = None
        self.hasobj = False

    def ret(self):
        return ( self.response, self.obj ) if self.hasobj else self.response


class _ResponseIterator(object):
    _global_counter = 0

    def __init__(self, maintainOrder, preprocessor):
        self._currentIndex = 0 if maintainOrder else None
        self._preprocessor = preprocessor
        self._responseAdded = Event()
        self._responses = OrderedDict()
        
        # A request is in-flight the moment it is popped off the request queue, until it is either added to this iterator or discarded (due to being killed)
        # The "done" variable is necessary to handle some corner cases, such as right at the beginning before the first request is in-flight
        self._inflight = 0
        self._done = False

        # Purely for debugging purposes to help differentiate the iterators
        # It's surprisingly frequent that new iterators will occupy exactly the same stop
        # in memory as a previous iterator, making id(self) not very useful.
        _ResponseIterator._global_counter += 1
        self._counter = _ResponseIterator._global_counter

    def __iter__(self):
        return self

    def _add(self, bundle, requestIndex):
        if self._responses is not None:
            self._responses[requestIndex] = bundle
        self._inflight -= 1
        #print('(Notify it.) %s [%d] %d, %s, %d, %s' % ( time(), self._counter, self._inflight, self._done, len(self._responses), bundle.request.url ))
        self._responseAdded.set()

    def next(self):
        while True:
            #print('(Loop it.  ) %s [%d] %d, %s, %d' % ( time(), self._counter, self._inflight, self._done, len(self._responses) ))
            if self._inflight == 0 and self._done and (self._responses is None or len(self._responses) == 0):
                raise StopIteration

            found = False
            if self._responses is not None and len(self._responses) > 0:
                if self._currentIndex is None:
                    found = True
                    bundle = self._responses.popitem(last = False)[1]
                else:
                    if self._currentIndex in self._responses:
                        found = True
                        bundle = self._responses.pop(self._currentIndex)
                        self._currentIndex += 1

            if found:
                #print('(Return it.) %s [%d] %d, %s, %d, %s' % ( time(), self._counter, self._inflight, self._done, len(self._responses), bundle.request.url ))
                if bundle.exception is None:
                    return self._preprocessor.success(bundle)
                else:
                    return self._preprocessor.error(bundle)
            else:
                #print('(Wait it.  ) %s [%d] %d, %s, %d' % ( time(), self._counter, self._inflight, self._done, len(self._responses) ))
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
        self.sortedqueue = [] # bundle, responseIterator, group, requestIndex, numTries, nextAttempt

    def add(self, bundle, responseIterator, group, requestIndex, numTries, wait):
        nextAttempt = time() + wait
        i = bisect_right(self.timelist, nextAttempt)
        self.timelist.insert(i, nextAttempt)
        self.sortedqueue.insert(i, ( bundle, responseIterator, group, requestIndex, numTries, nextAttempt ))

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


class _DefaultTimeoutHTTPAdapter(HTTPAdapter):
    def send(self, request, timeout = None, **kwargs):
        if timeout is None:
            timeout = self.defaultTimeout
        return super(_DefaultTimeoutHTTPAdapter, self).send(request, timeout = timeout, **kwargs)


class Requests(object):
    """A session of requests.

    Creates a session and a thread pool. The intention is to have one instance
    per server that you're hitting at the same time.

    :param concurrent: (optional) The maximum number of concurrent requests
                       allowed for this instance.
    :param minSecondsBetweenRequests: (optional) Every request is guaranteed to
                                      be separated by at least this many
                                      seconds.
    :param defaultTimeout: (Optional) Stop waiting after the server is
                           unresponsive for this many seconds.  See
                           `the requests docs <http://docs.python-requests.org/en/latest/user/quickstart/#timeouts>`_
                           for an exact definition of what constitutes a
                           timeout.  Should a timeout occur, the request will
                           either be retried or an exception will be raised
                           (depending on :param:`retryStrategy` and
                            :param:`responsePreprocessor`).
    :param retryStrategy: (optional) An instance of :class:`RetryStrategy` (or
                          subclass). Allows you to define if and how a request
                          should be retried on failure. The default
                          implementation (:class:`Strict`) retries failed
                          requests twice, for HTTP errors only, with at least
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
    def __init__(self, concurrent = 2, minSecondsBetweenRequests = 0.15, defaultTimeout = None, retryStrategy = Strict(), responsePreprocessor = ResponsePreprocessor()):
        if not isinstance(retryStrategy, RetryStrategy):
            raise TypeError('retryStrategy must be an instance of RetryStrategy, not %s' % type(retryStrategy))

        if not isinstance(responsePreprocessor, ResponsePreprocessor):
            raise TypeError('responsePreprocessor must be an instance of ResponsePreprocessor, not %s' % type(responsePreprocessor))

        self.session = Session()
        self.pool = Pool(concurrent)
        self.minSecondsBetweenRequests = minSecondsBetweenRequests
        self.retryStrategy = retryStrategy
        self.responsePreprocessor = responsePreprocessor

        self._adapter = _DefaultTimeoutHTTPAdapter()
        self.session.mount('http://', self._adapter)
        self.session.mount('https://', self._adapter)
        self._adapter.defaultTimeout = defaultTimeout

        self._requestGroups = 0
        self._requestAdded = Event()
        self._requestQueue = _RequestQueue()
        self._retryQueue = _RetryQueue()

        self._killed = False

        Requests._runningRequests.add(Greenlet.spawn(self._run))

    @property
    def defaultTimeout(self):
        return self._adapter.defaultTimeout

    @defaultTimeout.setter
    def defaultTimeout(self, value):
        self._adapter.defaultTimeout = value

    def __del__(self):
        self._kill()

    def _run(self):
        while True:
            try:
                self.pool.wait_available()
                reqGroup = self._requestQueue.getLatestGroup()
                retryGroup = self._retryQueue.getLatestGroup()

                if reqGroup is None and retryGroup is None:
                    if self._killed:
                        break
                    else:
                        self._requestAdded.clear()
                        self._requestAdded.wait(self._retryQueue.getMinWaitTime())
                        continue
                
                if retryGroup is None or (reqGroup is not None and reqGroup > retryGroup):
                    request, responseIterator, group, requestIndex = self._requestQueue.pop()
                    numTries = 0

                    if isinstance(request, tuple):
                        bundle = Bundle(request[0])
                        bundle.obj = request[1]
                        bundle.hasobj = True
                    else:
                        bundle = Bundle(request)

                    if self._skip(bundle):
                        responseIterator._add(bundle, requestIndex)
                        continue

                    try:
                        if isinstance(bundle.request, basestring):
                            bundle.request = Request(method = 'GET', url = bundle.request)
                        if isinstance(bundle.request, Request):
                            bundle.request = self.session.prepare_request(bundle.request)
                        if not isinstance(bundle.request, PreparedRequest):
                            raise TypeError('Request must be an instance of: str (or unicode), Request, PreparedRequest, not %s.' % type(bundle.request))
                    except Exception as ex:
                        # An exception here isn't recoverable, so don't bother testing for retries
                        bundle.exception = ex
                        bundle.traceback = exc_info()[2]
                        responseIterator._add(bundle, requestIndex)
                        continue
                else:
                    bundle, responseIterator, group, requestIndex, numTries = self._retryQueue.pop()

                #print('(Execute   ) %s [%d] %d, %s, %d, %s' % ( time(), responseIterator._counter, responseIterator._inflight, responseIterator._done, len(responseIterator._responses), bundle.request.url ))
                g = Greenlet(self._execute, bundle)
                # Attach data as a property, right on the greenlet.  This way, we won't lose the information if the greenlet is killed before it starts
                g.data = ( bundle, responseIterator, group, requestIndex, numTries )
                g.rawlink(self._response)
                self.pool.start(g)

                if self.minSecondsBetweenRequests > 0:
                    sleep(self.minSecondsBetweenRequests)

            except GreenletExit:
                self._kill()

    def _add(self, requestIterator, maintainOrder, responsePreprocessor):
        if responsePreprocessor is not None and not isinstance(responsePreprocessor, ResponsePreprocessor):
            raise TypeError('responsePreprocessor must be an instance of ResponsePreprocessor, not %s' % type(responsePreprocessor))
        responseIterator = _ResponseIterator(maintainOrder, responsePreprocessor or self.responsePreprocessor)
        self._requestQueue.add(requestIterator, responseIterator)
        self._requestAdded.set()
        return responseIterator

    def _skip(self, bundle):
        """Should the request be skipped altogether.  Return True to skip.

        If returning True, :attr:`bundle.response` should be set first.
        By default, this method always returns False, but it is useful to
        override if implementing something like a caching mechanism.
        """
        return False

    def _execute(self, bundle):
        try:
            bundle.response = self.session.send(bundle.request)
            self.retryStrategy.verify(bundle)
            bundle.exception = None
            bundle.traceback = None
        except (Exception, GreenletExit) as ex:
            bundle.exception = ex
            bundle.traceback = exc_info()[2]

        return None # bundle is already a property of the greenlet

    def _response(self, status):
        bundle, responseIterator, group, requestIndex, numTries = status.data
        numTries += 1

        #print('(Response  ) %s [%d] %d, %s, %d, %s' % ( time(), responseIterator._counter, responseIterator._inflight, responseIterator._done, len(responseIterator._responses), bundle.request.url ))

        if status.value is not None:
            if isinstance(status.value, GreenletExit):
                bundle.exception = status.value
            else:
                raise Exception('Unexpected return value of greenlet: %s' % status.value)

        if bundle.exception is None:
            responseIterator._add(bundle, requestIndex)
        elif isinstance(bundle.exception, GreenletExit):
            # Execution was killed in-flight
            responseIterator._inflight -= 1
            if responseIterator._inflight == 0:
                responseIterator._responseAdded.set()
        else:
            if hasattr(status, 'stopped'):
                # A stop was sent, so don't add to the retry queue regardless of strategy
                responseIterator._add(bundle, requestIndex)
            else:
                wait = self.retryStrategy.retry(bundle, numTries)
                if wait >= 0:
                    self._retryQueue.add(bundle, responseIterator, group, requestIndex, numTries, wait)
                    self._requestAdded.set()
                else:
                    responseIterator._add(bundle, requestIndex)

    def _kill(self):
        """Define the actions that should be taken when this object is killed."""
        self._killed = True

    _runningRequests = WeakSet()

    @staticmethod
    def _killall():
        killall(tuple(Requests._runningRequests))

    def one(self, request, responsePreprocessor = None):
        """Execute one request synchronously.

        Since this request is synchronous, it takes precedence over any other
        :meth:`each` or :meth:`swarm` calls which may still be processing.

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

        Subsequent calls to :meth:`each`, :meth:`swarm`, or :meth:`one` on the
        same :class:`Requests` instance will be prioritized *over* earlier
        calls. This is generally aligned with how responses are processed (one
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
                              this to False for a slight performance gain.
        :param responsePreprocessor: (optional) Override the default
                                     preprocessor for these requests only.
        :returns: A :class:`ResponseIterator` that may be iterated over to get a
                  :class:`requests.Response` for each request.
        """
        return self._add(iterable.__iter__(), maintainOrder, responsePreprocessor)

    def each(self, iterable, mapToRequest = (lambda i: i.request), maintainOrder = False, responsePreprocessor = None):
        """Execute a request for each object.

        Subsequent calls to :meth:`each`, :meth:`swarm`, or :meth:`one` on the
        same :class:`Requests` instance will be prioritized *over* earlier
        calls. This is generally aligned with how responses are processed (one
        response is inspected, which leads to more requests whose responses
        are inspected... etc.)

        This method will try hard to finish executing all requests, even if the
        iterator has fallen out of scope, or an exception was raised, or even
        if the execution of the main module is finished.  Use the :meth:`stop`
        method to cancel any pending requests and/or kill executing requests.

        :param iterable: A generator, list, tuple, dictionary, or any other
                         iterable.
        :param mapToRequest: (optional) By default, the `request` attribute (or
                         property) is used.  If such an attribute does not
                         exist (or some other behaviour is desired), this
                         function will be used to get a request for each object
                         in the iterable.
        :param maintainOrder: (optional) By default, the order is *not*
                              maintained between the iterable and the
                              responses.  Set this to True to guarantee that
                              the order is maintained.
        :param responsePreprocessor: (optional) Override the default
                                     preprocessor for these requests only.
        :returns: A :class:`ResponseIterator` that may be iterated over to get a
                  (:class:`requests.Response`, object) tuple for each object in
                  the given iterable.
        """
        return self._add(( ( mapToRequest(i), i ) for i in iterable ), maintainOrder, responsePreprocessor)

    def stop(self, killExecuting = True):
        """Stop the execution of requests early.

        The :meth:`swarm` and :meth:`each` methods will try hard to finish
        executing all requests, even if the iterator has fallen out of scope,
        or an exception was raised, or even if the execution of the main module
        is finished.

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

register(Requests._killall)
