# Bivalve - Bi-Directional Shell Sockets
This is a library implementing bi-directional shell sockets, a method of
IPC where applications send and receive shell-formatted commands over
simple TCP sockets.

It also serves as an  example of asynchronous input handling while maintaining a
connection with a remote service.  It shows effective methods of handling
connections and maintaining sessions with the `asyncio` socket utilities.
