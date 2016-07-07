#!/usr/bin/env python
# -*- coding: utf-8 -*-

# marty mcfly imports
from __future__ import absolute_import
from __future__ import unicode_literals

# stdlib imports
import datetime
import json

# third-party imports
import requests
from requests.exceptions import ChunkedEncodingError


# python 2/3 compatibility shim for checking if value is a text type
try:
    basestring  # attempt to evaluate basestring

    def isstr(s):
        return isinstance(s, basestring)
except NameError:
    def isstr(s):
        return isinstance(s, str)


# base url for all requests
BASE_URL = 'https://dweet.io'


def _check_stream_timeout(started, timeout):
    """Check if the timeout has been reached and raise a `StopIteration` if so.
    """
    if timeout:
        elapsed = datetime.datetime.utcnow() - started
        if elapsed.seconds > timeout:
            raise StopIteration


def _listen_for_dweets_from_response(response):
    """Yields dweets as received from dweet.io's streaming API
    """
    stream_buffer = ''
    open_brackets = 0

    for byte in response.iter_content():
        if byte == '{':
            open_brackets += 1

        if open_brackets > 0:
            stream_buffer += byte.decode('ascii')

            if byte == '}':
                open_brackets -= 1

        elif len(stream_buffer) > 0:

            print stream_buffer
            dweet = json.loads('"{}"'.format(stream_buffer))

            if isstr(dweet):
                yield json.loads(dweet)
            stream_buffer = ''


def listen_for_dweets_from(thing_name, timeout=900, key=None):
    """Create a real-time subscription to dweets
    """
    url = BASE_URL + '/listen/for/dweets/from/{0}'.format(thing_name)
    session = requests.Session()
    if key is not None:
        params = {'key': key}
    else:
        params = None

    start = datetime.datetime.utcnow()
    while True:
        request = requests.Request("GET", url, params=params).prepare()
        resp = session.send(request, stream=True, timeout=timeout)
        try:
            for x in _listen_for_dweets_from_response(resp):
                yield x
                _check_stream_timeout(start, timeout)
        except (ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
            pass
        _check_stream_timeout(start, timeout)
