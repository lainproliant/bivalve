# --------------------------------------------------------------------
# nio.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Thursday February 16, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import fcntl
import os
import selectors
import sys
from typing import Optional, TextIO


# --------------------------------------------------------------------
class NonBlockingTextInput:
    """
    Controller for non-blocking console text input.
    """

    def __init__(self, infile: TextIO = sys.stdin, promptfile: TextIO = sys.stdout):
        self.infile = infile
        self.promptfile = promptfile
        self.orig_fl: Optional[int] = None
        self.selector = selectors.DefaultSelector()
        self.timed_out = False

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.stop()

    def start(self):
        assert self.orig_fl is None
        self.orig_fl = fcntl.fcntl(self.infile, fcntl.F_GETFL)
        fcntl.fcntl(self.infile, fcntl.F_SETFL, self.orig_fl | os.O_NONBLOCK)
        self.selector.register(self.infile, selectors.EVENT_READ)

    def read(self, prompt: Optional[str] = None, timeout: Optional[int] = None):
        if prompt is not None and not self.timed_out:
            self.promptfile.write(prompt)
            self.promptfile.flush()
        result = self.selector.select(timeout=timeout)
        if result:
            self.timed_out = False
            return self.infile.read()
        else:
            self.timed_out = True
            return None

    def stop(self):
        assert self.orig_fl
        fcntl.fcntl(self.infile, fcntl.F_SETFL, self.orig_fl)
        self.orig_fl = None
