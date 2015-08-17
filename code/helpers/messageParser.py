#!/usr/bin/python3    pass
from struct import *
import ipaddress

DHTCommands = {
    500: "MSG_DHT_PUT",
    501: "MSG_DHT_GET",
    502: "MSG_DHT_TRACE",
    503: "MSG_DHT_GET_REPLY",
    504: "MSG_DHT_TRACE_REPLY",
    505: "MSG_DHT_ERROR"
}

DHTCommandsInv = {
    "MSG_DHT_PUT":  500,
    "MSG_DHT_GET": 501,
    "MSG_DHT_TRACE": 502,
    "MSG_DHT_GET_REPLY": 503,
    "MSG_DHT_TRACE_REPLY": 504,
    "MSG_DHT_ERROR": 505
}

class DHTMessage():
    def read_file(self, filename):
        with open(filename, "rb") as f:
            self.data = f.read()
        self.parse()
    pass

    def parse(self):
        commandNumber =  int.from_bytes( self.data[2:4], byteorder='big')
        command = DHTCommands[commandNumber]

        if command=="MSG_DHT_GET":
            self.message = DHTMessageGET(self.data, self.getSize())
        elif command=="MSG_DHT_PUT":
            self.message = DHTMessagePUT(self.data, self.getSize())
        elif command=="MSG_DHT_TRACE":
            self.message = DHTMessageTRACE(self.data, self.getSize())
        elif command=="MSG_DHT_TRACE_REPLY":
            self.message = DHTMessageTRACE(self.data, self.getSize())
        elif command=="MSG_DHT_GET_REPLY":
            self.message = DHTMessageGET_REPLY(self.data, self.getSize())
        elif command=="MSG_DHT_ERROR":
            self.message = DHTMessageERROR(self.data, self.getSize())
        else: # TODO: throw exception here
            pass

    def getSize(self):
        print("siye is", int.from_bytes( self.data[0:2], byteorder='big'))
        return  int.from_bytes( self.data[0:2], byteorder='big')

class DHTMessageParent():
    def __init__(self, data, size):
        self.data = data
        self.size = size

class DHTMessagePUT(DHTMessageParent):
    def get_key(self):
        return  int.from_bytes( self.data[4:12], byteorder='big')
    def get_ttl(self):
        return int.from_bytes( self.data[12:14], byteorder='big')
    def get_replication(self):
        return int.from_bytes( self.data[14:15], byteorder='big')
    def get_reserved(self):
        return self.data[15:20]
    def get_content(self):
        return self.data[20:self.size]

class DHTMessageGET(DHTMessageParent):
    def get_key(self):
        return  int.from_bytes( self.data[4:12], byteorder='big')

class DHTMessageTRACE(DHTMessageParent):
    def get_key(self):
        return  int.from_bytes( self.data[4:12], byteorder='big')

class DHTMessageGET_REPLY:
    def __init__(self, key, content ):
        frame = bytearray()

        size = int(16+16+256)
        size = int(size / 8) + len(content)

        frame += size.to_bytes(2, byteorder='big')
        frame += (503).to_bytes(2, byteorder='big') # 503 is MSG_DHT_GET?REPLY
        frame += key.to_bytes(32, byteorder='big')
        frame += (content).to_bytes(len(content), byteorder='big')
        self.frame = frame

    def get_data(self):
        return self.frame

class DHTMessageTRACE_REPLY:
    def __init__(self, key, hops ):
        frame = bytearray()

        size = int(16+256+len(hops)*(256+32+128))
        size = int(size / 8) # convert to byte as size should be byte instead of bit

        frame += size.to_bytes(2, byteorder='big')
        frame += (504).to_bytes(2, byteorder='big') # 504 is MSG_DHT_TRACE_REPLY
        frame += key.to_bytes(32, byteorder='big')

        for hop in hops:
            frame += hop.as_bytes()
        self.frame = frame

    def get_data(self):
        return self.frame

class DHTHop:
    def __init__(self, peerId, kxPort, IPv4Address, IPv6Address):

        self.peerId =  peerId.to_bytes(32, byteorder='big')
        self.kxPort =  kxPort.to_bytes(2, byteorder='big')
        self.reserved = (0).to_bytes(2, byteorder='big')

        ipv4 =  ipaddress.ip_address(IPv4Address).packed
        ipv6 =  ipaddress.ip_address(IPv6Address).packed

        self.IPv4Address = ipv4
        self.IPv6Address = ipv6

    def as_bytes(self):
        frame = bytearray()
        frame +=(self.peerId)
        frame +=(self.kxPort)
        frame +=(self.reserved)
        frame +=(self.IPv4Address)
        frame +=(self.IPv6Address)
        return frame;

class DHTMessageERROR:
    def __init__(self, requestType, requestKey ):
        frame = bytearray()

        size = int(64+256)
        size = int(size / 8) # convert to byte as size should be byte instead of bit

        frame += size.to_bytes(2, byteorder='big')
        frame += (505).to_bytes(2, byteorder='big') # 505 is MSG_DHT_ERROR
        frame += key.to_bytes(32, byteorder='big')
        frame += (requestType).to_bytes(2, byteorder='big')  # unused
        frame += (requestKey).to_bytes(32, byteorder='big')
        self.frame = frame

    def get_data(self):
        return self.frame
