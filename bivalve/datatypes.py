# --------------------------------------------------------------------
# datatypes.py
#
# Author: Lain Musgrove (lain.musgrove@gmail.com)
# Date: Monday July 31, 2023
# --------------------------------------------------------------------

import asyncio

ArgV = list[str]
ArgVQueue = asyncio.Queue[ArgV]
