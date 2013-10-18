# -*- coding: utf-8 -*-
"""
Simple-requests allows you to get the performance benefit of asynchronous
requests, without needing to use any asynchronous coding paradigms.

Usage
-----

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
"""

from setuptools import setup

setup(
    name='simple-requests',
    version='1.0.0',
    url='https://github.com/ctheiss/simple-requests',
    license='MIT',
    author='Corey Theiss',
    author_email='corey@exploringsolutions.com',
    description='Asynchronous requests in Python without thinking about it',
    long_description=__doc__,
    install_requires=[
        'gevent',
        'requests'
    ],
    py_modules=['simple-requests'],
    zip_safe=False,
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