# --------------------------------------------------------------------
# test_nio.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Sunday February 12, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------

import sys
import time
import threading
from dataclasses import dataclass, field

from .util import NIOTextInput


# --------------------------------------------------------------------
@dataclass
class State:
    ninput: NIOTextInput
    shutdown: threading.Event = field(default_factory=threading.Event)


# --------------------------------------------------------------------
def input_thread(state: State):
    while not state.shutdown.is_set():
        s = state.ninput.read("nio> ", timeout=1)
        if s is not None:
            s = s.strip()
            if s == "quit":
                state.shutdown.set()
            print(s)


# --------------------------------------------------------------------
def main():
    ninput = NIOTextInput()
    ninput.start()
    state = State(ninput)
    thread = threading.Thread(target=input_thread, args=(state,))
    thread.start()

    try:
        while not state.shutdown.is_set():
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        state.shutdown.set()

    finally:
        thread.join()

    ninput.stop()


# --------------------------------------------------------------------
if __name__ == "__main__":
    main()
