#!/usr/bin/python3

class DHTMessage():
    def read_file(self, filename):
        with open(filename, "rb") as f:
            self.data = f.read()
        self.parse()
    pass

    def parse(self):
        command =  self.data[16:32].decode("utf-8").strip()
        print(command)
        if command=="MSG_DHT_GET":
            self.message = DHTMessageGET(self.data, self.getSize())
        elif command=="MSG_DHT_PUT":
            self.message = DHTMessagePUT(self.data, self.getSize())
        elif command=="MSG_DHT_TRACE":
            self.message = DHTMessageGET_REPLY(self.data, self.getSize())
        elif command=="MSG_DHT_TRACE_REPLY":
            self.message = DHTMessageTRACE(self.data, self.getSize())
        elif command=="MSG_DHT_GET_REPLY":
            self.message = DHTMessageTRACE_REPLY(self.data, self.getSize())
        elif command=="MSG_DHT_ERROR":
            self.message = DHTMessageERROR(self.data, self.getSize())
        else: # TODO: throw exception here
            pass

    def getSize(self):
        return int(self.data[0:16].decode("utf-8"))

class DHTMessageParent():
    def __init__(self, data, size):
        self.data = data
        self.size = size

class DHTMessagePUT(DHTMessageParent):
    def get_key(self):
        return self.data[32:64].decode("utf-8")
    def get_ttl(self):
        return int(self.data[64:80].decode("utf-8"))
    def get_replication(self):
        return self.data[80:88].decode("utf-8")
    def get_reserved(self):
        return self.data[88:128].decode("utf-8")
    def get_content(self):
        return self.data[128:128+self.size].decode("utf-8")

class DHTMessageGET(DHTMessageParent):
    def get_key(self):
        return self.data[32:288].decode("utf-8")

class DHTMessageGET_REPLY:
    pass

class DHTMessageTRACE:
    pass

class DHTMessageTRACE_REPLY:
    pass

class DHTMessageERROR:
    pass
