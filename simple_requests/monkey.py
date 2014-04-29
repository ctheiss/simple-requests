# -*- coding: utf-8 -*-

from httplib import IncompleteRead

from gevent import sleep, spawn
from gevent.lock import BoundedSemaphore

from requests.packages.urllib3.connection import HTTPConnection, HTTPSConnection

# Allow (globally) up to 200 open connections before severely throttling.
# There are some corner-cases whereby connections are not closed immediately,
# or where there are legitimately more than 200 open connections.  For example:
# requests.swarm(urls1):
#     requests.swarm(urls2):
#         ...
#         requests.swarm(urls200):
#             requests.one(url201)
#
# As a compromise between avoiding the "Too many open files" error and
# deadlocking, we'll throttle connection creation after the first 200 open
# connections instead of simply blocking forever.  Closing connections down to
# below 200 will eliminate the throttling. After the first 200, connections
# will open only after waiting 10 seconds.
#
# Note that this timeout applies in parallel, so practically it will mean that
# max(requests.concurrent) is the upper bound of new connections every
# 10 seconds.


def patch(allowIncompleteResponses = False, avoidTooManyConnections = False):
    if allowIncompleteResponses:
        _patch_allowIncompleteResponses()
    else:
        _unpatch_allowIncompleteResponses()
    
    if avoidTooManyConnections:
        _patch_avoidTooManyConnections()
    else:
        _unpatch_avoidTooManyConnections()


_applied = {
    'allowIncompleteResponses': False,
    'avoidTooManyConnections': False
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

        while len(self._semaphore._links) > 0:
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
            if len(self._semaphore._links) > 0:
                self._timer = spawn(self._leak)


def _patch_allowIncompleteResponses():
    if not _applied['allowIncompleteResponses']:
        _applied['allowIncompleteResponses'] = True
        def _simple_new_read(self, amt = None):
            try:
                return self._simple_old_read(amt)
            except IncompleteRead as e:
                return e.partial
        httplib.HTTPResponse._simple_old_read = httplib.HTTPResponse.read
        httplib.HTTPResponse.read = _simple_new_read

def _unpatch_allowIncompleteResponses():
    if _applied['allowIncompleteResponses']:
        _applied['allowIncompleteResponses'] = False
        httplib.HTTPResponse.read = httplib.HTTPResponse._simple_old_read
        del httplib.HTTPResponse._simple_old_read


def _patch_avoidTooManyConnections(softMaxConnections = 200, maxSeconds = 10):
    if not _applied['avoidTooManyConnections']:
        _applied['avoidTooManyConnections'] = True
        openConnections = _LeakySemaphore(softMaxConnections, maxSeconds)


        def _simple_new_connect(self):
            if not hasattr(self, '_simple_acquire'):
                self._simple_acquire = True
                openConnections.acquire()
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

def _unpatch_avoidTooManyConnections():
    if _applied['avoidTooManyConnections']:
        _applied['avoidTooManyConnections'] = False

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
