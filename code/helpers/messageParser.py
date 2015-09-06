#!/usr/bin/python3

"""
The message parser parses and generates binary messages to communicate with other modules like the KX module.
"""

from struct import *
import ipaddress # imported here as sphynx  documentation generator crashes if it is written on top
from jsonschema import validate


DHTCommands = {
    500: "MSG_DHT_PUT",
    501: "MSG_DHT_GET",
    502: "MSG_DHT_TRACE",
    503: "MSG_DHT_GET_REPLY",
    504: "MSG_DHT_TRACE_REPLY",
    505: "MSG_DHT_ERROR"
}

# the inverse dict, unfortunately python does not have a built in function to solve it another way
DHTCommandsInv = {
    "MSG_DHT_PUT":  500,
    "MSG_DHT_GET": 501,
    "MSG_DHT_TRACE": 502,
    "MSG_DHT_GET_REPLY": 503,
    "MSG_DHT_TRACE_REPLY": 504,
    "MSG_DHT_ERROR": 505
}

class DHTMessage():
    """
    Base class for other classes representing incoming data such as as ``DHTMessagePUT``
    """
    def __init__(self):
        self.message = None


    def read_file(self, filename):
        """Read a binary file representing a message. The message is automatically parsed
        afterwards.

        :param filename: the location of the file
        """
        with open(filename, "rb") as f:
            self.data = f.read()

        return self.parse()

    def read_binary(self, data):
        """Read and parse binary data

        :param data: The data
        :type data: bytearray
        """
        self.data = data
        return self.parse()

    def parse(self):
        """
        Parse the message

        ``self.message`` will automatically become the type of message specified with the command number (``DHTMessageGET``, ``DHTMessagePUT`` etc.)
        """
        commandNumber =  int.from_bytes( self.data[2:4], byteorder='big')
        command = DHTCommands[commandNumber]

        if command=="MSG_DHT_GET":
            self.message = DHTMessageGET(self.data, self.getSize())
        elif command=="MSG_DHT_PUT":
            self.message = DHTMessagePUT(self.data, self.getSize())
        elif command=="MSG_DHT_TRACE":
            self.message = DHTMessageTRACE(self.data, self.getSize())
        elif command=="MSG_DHT_ERROR":
            self.message = DHTMessageERROR(self.data, self.getSize())
        else: # TODO: throw exception here
            pass

        self.message.command = command

        return self.message

    def is_valid(self):
        try:
            validate(self.message.make_dict(), SCHEMA_MSG_DHT[self.message.command])
            return True
        except:
            return False

    def get_validation_execption(self):
        try:
            validate(self.message.make_dict(), SCHEMA_MSG_DHT[self.message.command])
            return None
        except Exception as e:
            return str(e)

    def getSize(self):
        """
        Returns the size of the message

        :returns: Message Size in bytes
        :rtype: int

        """
        return  int.from_bytes( self.data[0:2], byteorder='big')

class DHTMessageParent():
    def __init__(self, data, size):
        self.data = data
        self.size = size


class DHTMessagePUT(DHTMessageParent):
    """
    Provides additional parameters for a DHTMessage which is a PUT message
    """
    def make_dict(self):
        return {
            "ttl" : self.get_ttl(),
            "key" : self.get_key(),
            "replication" : self.get_replication(),
            "content_length" : len(self.get_content())
        }

    def get_key(self):
        """
        Returns the key as integer

        :rtype: int
        """
        return  int.from_bytes( self.data[4:36], byteorder='big')

    def get_ttl(self):
        """
        Returns the time to live (ttl) in seconds

        :rtype: int
        """
        return int.from_bytes( self.data[36:38], byteorder='big')

    def get_replication(self):
        """
        Returns the replication

        :rtype: int
        """
        return int.from_bytes( self.data[38:39], byteorder='big')

    def get_reserved(self):
        return self.data[39:44]

    def get_content(self):
        """
        Returns the content

        :returns: content
        :rtype: bytearray
        """
        return self.data[44:self.size]

class DHTMessageGET(DHTMessageParent):
    """
    Provides additional parameters for a DHTMessage which is a GET message.
    """
    def make_dict(self):
        return {
            "key" : self.get_key()
        }

    def get_key(self):
        """
        Returns the key as integer

        :rtype: int
        """
        return int.from_bytes(self.data[4:36], byteorder='big')

class DHTMessageTRACE(DHTMessageParent):
    """
    Provides additional parameters for a DHTMessage which is a TRACE message.
    """
    def make_dict(self):
        return {
            "key" : self.get_key()
        }

    def get_key(self):
        """
        Returns the key as integer

        :rtype: int
        """
        return int.from_bytes(self.data[4:36], byteorder='big')

class DHTMessageGET_REPLY:
    """
    Initializes a ``MSG_DHT_GET_REPLY`` message to send later.

    :param key: the key as integer
    :param content: the content of the get query in binary format.
    """
    def __init__(self, key, content):
        assert type(content) is bytes

        frame = bytearray()

        size = int(16+16+256)
        size = int(size / 8) + len(content)

        frame += size.to_bytes(2, byteorder='big')
        frame += (503).to_bytes(2, byteorder='big') # 503 is MSG_DHT_GET?REPLY
        frame += key.to_bytes(32, byteorder='big')
        frame += content
        self.frame = frame

    def get_data(self):
        """
        Returns the data in binary format

        :rtype: bytearray
        """
        return self.frame

class MAKE_MSG_DHT_GET:
    """
    Initializes a `MSG_DHT_GET`` message to send later.

    :param key: the key as integer
    :param hops: a list of :py:meth:`DHTHop` objects
    """
    def __init__(self, key):

        size = 40
        frame = bytearray()
        frame += size.to_bytes(2, byteorder='big')
        frame += (501).to_bytes(2, byteorder='big') # 501 is MSG_DHT_GET
        frame += int(key).to_bytes(32, byteorder='big')

        self.frame = frame

    def get_data(self):
        return self.frame

class MAKE_MSG_DHT_PUT:
    """
    Initializes a ``MSG_DHT_PUT`` message to send later.

    :param key: key as integer
    :type key: int
    :param content: The content to be stored
    :type content: bytearray
    :param ttl: time the content is available in seconds (43200 per default)
    :type ttl: int
    :param replication: The amount of replication. A replication degree of three means a tripple redundancy. If one node crashes, there are still two nodes available for example.
    :type replication: int
    """
    def __init__(self, key, content, ttl=43200,replication=3):

        frame = bytearray()
        size = 44+len(content)
        frame += size.to_bytes(2, byteorder='big')
        frame += (500).to_bytes(2, byteorder='big') # 500 is MSG_DHT_PUT
        frame += int(key).to_bytes(32, byteorder='big')
        frame += int(ttl).to_bytes(2, byteorder='big')
        frame += int(replication).to_bytes(1, byteorder='big') # replication
        frame += int(0).to_bytes(1, byteorder='big') # reserved
        frame += int(0).to_bytes(4, byteorder='big') # reserved
        frame += content # content

        self.frame = frame
    """
        :returns: Message in binary format
        :rtype: bytearray
    """
    def get_data(self):
        return self.frame

class MAKE_MSG_DHT_TRACE_REPLY:
    """
    Initializes a ``MSG_DHT_TRACE_REPLY`` message to send later.

    :param key: the key as integer
    :param hops: a list of :py:meth:`DHTHop` objects
    """
    def __init__(self, key, hops):

        frame = bytearray()

        size = int(32+256+len(hops)*(256+32*2+128))
        assert size < 65536  # Size field only has length of 2 bytes (2^16 bits)
        size = int(size / 8) # convert to byte as size should be byte instead of bit

        frame += size.to_bytes(2, byteorder='big')
        frame += (504).to_bytes(2, byteorder='big') # 504 is MSG_DHT_TRACE_REPLY
        frame += key.to_bytes(32, byteorder='big')

        for i, hop in enumerate(hops):
            frame += hop.as_bytes()
        self.frame = frame

    def get_data(self):
        """
        Return the binary representation

        :returns: Message in binary format
        :rtype: bytearray
        """
        return self.frame

class DHTHop:
    """
    A DHT Hop for ``MSG_DHT_TRACE_REPLY`` message

    :param peerId: the peer id with a maximum length of 32 bytes
    :type peerId: Int
    :param kxPort: the kx port with a maxmimum length of 2 bytes
    :type kxPort: Int
    :param IPv4Address: For example 192.168.0.1
    :type IPv4Address: String
    :param IPv6Address: For example FE80:0000:0000:0000:0202:B3FF:FE1E:8329
    :type IPv6Address: String
    """
    def __init__(self, peerId, kxPort, IPv4Address, IPv6Address):

        self.peerId =  peerId.to_bytes(32, byteorder='big')
        self.kxPort =  kxPort.to_bytes(2, byteorder='big')
        self.reserved = (0).to_bytes(2, byteorder='big')

        ipv4 =  ipaddress.ip_address(IPv4Address).packed
        ipv6 =  ipaddress.ip_address(IPv6Address).packed

        self.IPv4Address = ipv4
        self.IPv6Address = ipv6

    """
    Return the binary representation of a DHT Hop, which can be appended to a trace Message

    :returns: DHTHop in binary format
    :rtype: bytearray
    """
    def as_bytes(self):
        frame = bytearray()
        frame += self.peerId
        frame += self.kxPort
        frame += self.reserved
        frame += self.IPv4Address
        frame += self.IPv6Address

        return frame

class DHTMessageERROR:
    """
    Generates an error message

    :param requestType: The type of request
    :type requestType: int
    :param requestKey: The key
    :type requestKey: int
    """
    def __init__(self, requestType, requestKey):
        frame = bytearray()

        size = int(32*2+256)
        size = int(size / 8) # convert to byte as size should be byte instead of bit

        frame += size.to_bytes(2, byteorder='big')
        frame += (505).to_bytes(2, byteorder='big') # 505 is MSG_DHT_ERROR
        frame += requestKey.to_bytes(32, byteorder='big')
        frame += requestType.to_bytes(2, byteorder='big')  # unused
        frame += requestKey.to_bytes(32, byteorder='big')
        self.frame = frame

    def get_data(self):
        """
        Return the binary representation

        :returns: Message in binary format
        :rtype: bytearray
        """
        return self.frame
