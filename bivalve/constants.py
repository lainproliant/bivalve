# --------------------------------------------------------------------
# constants.py
#
# Author: Lain Musgrove (lain.proliant@gmail.com)
# Date: Tuesday May 9, 2023
#
# Distributed under terms of the MIT license.
# --------------------------------------------------------------------


# --------------------------------------------------------------------
class ControlCommands:
    PREFIX = "_ctl"
    ACK = PREFIX + "_ack"
    BYE = PREFIX + "_bye"
    CALL = PREFIX + "_call"
    FAIL = PREFIX + "_fail"
    RETURN = PREFIX + "_return"
    SYN = PREFIX + "_syn"


# --------------------------------------------------------------------
class CallFailureTypes:
    INVALID_PARAMETERS = "InvalidParameters"
    INVALID_RETURN = "InvalidReturn"
    TERMINATED = "Terminated"
    TIMEOUT = "Timeout"
    UNKNOWN_FUNCTION = "UnknownFunction"
