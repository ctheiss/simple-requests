# -*- coding: utf-8 -*-

from compat import HTTPError

class RetryStrategy(object):
    """The base implementation, which doesn't retry at all."""
    def verify(self, bundle):
        if bundle.response.status_code >= 400:
            raise HTTPError(bundle.response)

    def retry(self, bundle, numTries):
        return -1

class Strict(RetryStrategy):
    """A good default :class:`RetryStrategy`.

    Retries up to two times, with 2 seconds between each attempt.

    Only HTTP errors are retried.
    """
    def retry(self, bundle, numTries):
        if isinstance(bundle.exception, HTTPError) and numTries < 3:
            return 2
        else:
            return -1

class Lenient(RetryStrategy):
    """A :class:`RetryStrategy` designed for very small servers.

    Small servers are expected to go down every now and then, so this
    strategy retries requests that returned an HTTP error up to 4 times,
    with a full minute between each
    attempt.

    Other (non-HTTP) errors are retried only once after 60 seconds.
    """
    def retry(self, bundle, numTries):
        if numTries == 1 or (isinstance(bundle.exception, HTTPError) and numTries < 5):
            return 60
        else:
            return -1

class Backoff(RetryStrategy):
    """A :class:`RetryStrategy` designed for APIs.

    Since APIs are *expected* to work, this implementation retries requests
    that return HTTP errors many times, with an exponentially-increasing
    time in between each request (capped at 60 seconds).

    Other (non-HTTP) errors are retried only once after 10 seconds.
    """
    def retry(self, bundle, numTries):
        if isinstance(bundle.exception, HTTPError):
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
