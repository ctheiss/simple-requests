#!/usr/bin/env python
# -*- coding: utf-8 -*-

class separate_library(object):
    def __init__(self):
        import gevent.monkey; gevent.monkey.patch_all(thread=False, select=False)

    def do_work(self):
        from gevent import spawn
        spawn(self._do)

    def _do(self):
        from gevent import sleep
        sleep(1)
        print 'Done!'

if __name__ == '__main__':
    lib = separate_library()
    lib.do_work()
