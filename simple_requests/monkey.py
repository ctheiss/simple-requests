# -*- coding: utf-8 -*-

"""This module exports one function: patch.  Each argument can be True to
perform the patch, or False to "unpatch".

Due to the nature of the problems these patches are intended to fix,
misbehaving servers, they are very difficult to test.  Please take this
into consideration when using them in a production environment.

allowIncompleteResponses
------------------------
Affects [httplib](https://docs.python.org/2/library/httplib.html).  Allows
continued processing when the actual amount of data is different than the
amount specified ahead of time by the server (using the content-length
header).  What are the possible scenarios?
- The server calculated the content-length incorrectly, and you actually got
  all the data.  This patch will fix this scenario.  This happens surprisingly
  often.
- You didn't get the full payload.  In this case, you'll likely get a
  `ContentDecodingError` if the response has been compressed, or truncated
  content (which may be chopped in the middle of a multi-byte character,
  resulting in a `UnicodeError`). If you're parsing structured data, like XML
  or JSON, it will almost certainly be invalid. So, in many cases, you'll get
  an error raised anyways.

Note that this patch affects **all** python http connections, even those
outside of simple-requests, requests, and urllib3.

avoidTooManyOpenFiles
-----------------------
Affects [urllib3](https://github.com/shazow/urllib3).  simple-requests
ultimately uses the extremely clever urllib3 library to manage connection
pooling.  This library implements a *leaky bucket* paradigm for handling
pools.  What does this mean?  Some number of connections are kept open
with each server to avoid the costly operation of opening a new one.
Should the maximum number of connections be exceeded, for whatever reason,
a new connection will be created.  This *bonus connection* will be discarded
once it's job is done; it will not go back into the pool.  This is considered
to be a good compromise between performance and number of open connections
(which count as open files).

There are some scenarios whereby the number of open connections keeps
increasing faster than they can be closed, and eventually, you get
`socket.error: [Errno 24] Too many open files`.  After this point, it's
probably unrecoverable.

How many open connections can you have before this is a problem?  On many
systems, around 900.

This patch will add a speed-limit to the creation of new `urllib3` connections.
As long as there are fewer than 200 open connections, new ones will be
created immediately.  After that point, new connections are opened at a rate
of 1 every 10 seconds.  Once the number of open connections drops to below
200, they are created immediately again.

In addition to the speed-limit, for every 200 connections opened after 200 are
already open, the garbage collector is forcefully run.  Testing has shown that
this can help close sockets lingering in a CLOSE_WAIT state (which counts as an
open file).

Why is a speed-limit used instead of just blocking new connections from
being opened?  Because there are scenarios where this would cause a deadlock:

    for r1 in requests.swarm(urls1):
        for r2 in requests.swarm(urls2):
            ...
            for r200 in requests.swarm(urls200):
                requests.one(url201)

If the problem is that a server is responding incredibly slowly to a swarm of
requests, and even the speed limit isn't helping, your best options are:
 - Restructure your program to avoid nesting swarms/each.
 - Drastically decrease `concurrent`, increase `minSecondsBetweenRequests`, or
   add a `defaultTimeout` (all parameters to `Requests`).
"""

from gc import collect

from compat import HTTPResponse, IncompleteRead

from gevent import sleep, spawn
from gevent.lock import BoundedSemaphore

from requests.packages.urllib3.connection import HTTPConnection, HTTPSConnection

def patch(allowIncompleteResponses = False, avoidTooManyOpenFiles = False):
    if allowIncompleteResponses:
        _patch_allowIncompleteResponses()
    else:
        _unpatch_allowIncompleteResponses()
    
    if avoidTooManyOpenFiles:
        _patch_avoidTooManyOpenFiles()
    else:
        _unpatch_avoidTooManyOpenFiles()


_applied = {
    'allowIncompleteResponses': False,
    'avoidTooManyOpenFiles': False
}


class _LeakySemaphore(object):
    def __init__(self, value = 1, maxSeconds = 10):
        self._semaphore = BoundedSemaphore(value)
        self._maxSeconds = maxSeconds
        self._timer = None
        self._leaked = 0
        self._stopped = False

    def _leak(self):
        sleep(self._maxSeconds)
        self._leaked += 1
        self._semaphore.release()

    @property
    def inUse(self):
        return self._semaphore._initial_value - self.semaphore.counter

    @property
    def waiting(self):
        return len(self._semaphore._links)

    def release(self):
        if self._stopped:
            return
        if self._leaked > 0:
            self._leaked -= 1
        else:
            self._semaphore.release()

    def stop(self):
        self._stopped = True

        if self._timer is not None:
            self._timer.kill(block = False)
            self._timer = None

        while self.waiting > 0:
            self._semaphore.release()
            sleep(0.1)

    def acquire(self):
        if self._stopped:
            return
        if self._semaphore.locked() and not self._timer:
            self._timer = spawn(self._leak)
        self._semaphore.acquire(blocking = True, timeout = None)
        if self._timer is not None:
            self._timer.kill(block = False)
            self._timer = None
            if self.waiting > 0:
                self._timer = spawn(self._leak)


def _patch_allowIncompleteResponses():
    if not _applied['allowIncompleteResponses']:
        _applied['allowIncompleteResponses'] = True
        def _simple_new_read(self, amt = None):
            try:
                return self._simple_old_read(amt)
            except IncompleteRead as e:
                return e.partial
        HTTPResponse._simple_old_read = HTTPResponse.read
        HTTPResponse.read = _simple_new_read

def _unpatch_allowIncompleteResponses():
    if _applied['allowIncompleteResponses']:
        _applied['allowIncompleteResponses'] = False
        HTTPResponse.read = HTTPResponse._simple_old_read
        del HTTPResponse._simple_old_read


def _patch_avoidTooManyOpenFiles(softMaxConnections = 200, maxSeconds = 10, forceGCInterval = 200):
    if not _applied['avoidTooManyOpenFiles']:
        _applied['avoidTooManyOpenFiles'] = True
        openConnections = _LeakySemaphore(softMaxConnections, maxSeconds)
        openConnections.forceGCCounter = 0

        def _simple_new_connect(self):
            if not hasattr(self, '_simple_acquire'):
                self._simple_acquire = True
                openConnections.acquire()
                if openConnections.waiting > 0:
                    if openConnections.forceGCCounter > forceGCInterval:
                        openConnections.forceGCCounter = 0
                        collect()

            if hasattr(self, '_simple_old_connect'):
                self._simple_old_connect()
            else:
                super(HTTPConnection, self).connect()

        if hasattr(HTTPConnection, 'connect'):
            HTTPConnection._simple_old_connect = HTTPConnection.connect
        HTTPConnection.connect = _simple_new_connect

        if hasattr(HTTPSConnection, 'connect'):
            HTTPSConnection._simple_old_connect = HTTPSConnection.connect
            HTTPSConnection.connect = _simple_new_connect


        def _simple_new_close(self):
            if hasattr(self, '_simple_acquire'):
                del self._simple_acquire
                openConnections.release()
            if hasattr(self, '_simple_old_close'):
                self._simple_old_close()
            else:
                super(HTTPConnection, self).close()

        if hasattr(HTTPConnection, 'close'):
            HTTPConnection._simple_old_close = HTTPConnection.close
        HTTPConnection.close = _simple_new_close

        if hasattr(HTTPSConnection, 'close'):
            HTTPSConnection._simple_old_close = HTTPSConnection.close
            HTTPSConnection.close = _simple_new_close

def _unpatch_avoidTooManyOpenFiles():
    if _applied['avoidTooManyOpenFiles']:
        _applied['avoidTooManyOpenFiles'] = False

        openConnections.stop()

        if hasattr(HTTPConnection, '_simple_old_connect'):
            HTTPConnection.connect = HTTPConnection._simple_old_connect
            del HTTPConnection._simple_old_connect
        else:
            del HTTPConnection.connect

        if hasattr(HTTPSConnection, '_simple_old_connect'):
            HTTPSConnection.connect = HTTPSConnection._simple_old_connect
            del HTTPSConnection._simple_old_connect


        if hasattr(HTTPConnection, '_simple_old_close'):
            HTTPConnection.close = HTTPConnection._simple_old_close
            del HTTPConnection._simple_old_close
        else:
            del HTTPConnection.close

        if hasattr(HTTPSConnection, '_simple_old_close'):
            HTTPSConnection.close = HTTPSConnection._simple_old_close
            del HTTPSConnection._simple_old_close
