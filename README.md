# simple-requests

Asynchronous requests in Python without thinking about it.

## Usage

```python
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
```

Please visit [pythonhosted](http://pythonhosted.org/simple-requests/) for the complete documentation, including API reference.

## Installation

Installation is easy with pip:

```
$ pip install simple-requests
```

## Goal

The goal of this library is to allow you to get the performance benefit of asynchronous requests, without needing to use any asynchronous coding paradigms.  It is built on [gevent](https://github.com/surfly/gevent) and [requests](https://github.com/kennethreitz/requests).

If you like getting your hands dirty, the `gevent.Pool` and `requests.Session` that drives the main object is readily available for you to tinker with as much as you'd like.

## Features

There is also some added functionality not available out-of-the-box from the base libraries:
* Request thresholding
* Automatic retry on failure, with three different retry strategies included that focus on different applications (big server scrape, small server scrape, API)
* Lazy loading and minimal object caching to keep the memory footprint down
