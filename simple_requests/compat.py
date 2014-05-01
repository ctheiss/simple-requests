# -*- coding: utf-8 -*-

import sys

if sys.version_info[0] >= 3:
    from http.client import HTTPResponse, IncompleteRead
    from urllib.error import HTTPError as _HTTPError
else:
    from httplib import HTTPResponse, IncompleteRead
    from urllib2 import HTTPError as _HTTPError

class HTTPError(_HTTPError):
    """Encapsulates HTTP errors (status codes in the 400s and 500s).

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
