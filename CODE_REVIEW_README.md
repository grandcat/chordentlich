Code Review Readme
=============================================
Our current solution embodies the (async) infrastructure
for sending and receiving RPC
and parts of the Chord functionality like finger tables.
It is in an very early stage and a lot of functionality
is still missing yet. This is due to the fact that we first had
to become familiar with specific aspects about
Chord and the python network apis.
Still, we would be happy to receive some feedback about
the current state of this project.

For Gnome 3 use start.sh to start the project,
for XFCE use start_xfce.sh to start the project.
Currently we emulate a network by using different ports
on localhost as node addresses.

The main functionality is located in dhtServer.py.
FingerTable.py contains additional classes for handeling
nodes and finger tables. dhtServer starts a server who can
receive and send commands such as FIND_SUCCESSOR.

Commands are received in the "data_received" method of
the DHTAsyncServer class and
parsed dependend on their JSON "action" property.

One thing that is special about our implementation is
that it is planned to be non blocking.
That means, while performing an command such as
FIND_SUCCESSOR we can process other commands while
FIND_SUCCESSOR contacts other nodes.
