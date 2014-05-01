# -*- coding: utf-8 -*-
"""
Simple-requests allows you to get the performance benefit of asynchronous
requests, without needing to use any asynchronous coding paradigms.

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

Release History
---------------
1.1.0 (May 01, 2014)
======================
**API Changes**
 * ``defaultTimeout`` parameter added to ``Requests.__init__``
**Bug Fixes**
 * No more errors / warnings on exit
 * Fixed due to API changes in gevent 1.0
 * Fixed a couple documentation errors
**Features**
 * Added a patch class, with monkey patches of urllib3 (to reduce the likelihood of too many
   open connections/files at once) and httplib (to disregard servers that incorrectly report the content-length)
"""

from setuptools import setup

# python setup.py sdist register upload
# Choice 1
# Save login: yes
# After upload, delete file ~/.pypirc

setup(
    name='simple-requests',
    version='1.1.0',
    url='https://github.com/ctheiss/simple-requests',
    license='MIT',
    author='Corey Theiss',
    author_email='corey@exploringsolutions.com',
    description='Asynchronous requests in Python without thinking about it.',
    long_description=__doc__,
    install_requires=[
        'gevent >= 1.0',
        'requests >= 2.1.0'
    ],
    packages=['simple_requests'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)