#!/usr/bin/python3

class DHTMessage():
    def read_file(self, filename):
        with open(filename, "rb") as f:
            self.data = f.read()
    pass

    def make(self):
        command =  msg2.data[16:32].decode("utf-8").strip()
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
        return int(msg2.data[0:16].decode("utf-8"))

class DHTMessageParent():
    def __init__(self, data, size):
        self.data = data
        self.size = size

class DHTMessagePUT(DHTMessageParent):
    def get_key(self):
        return self.data[32:288].decode("utf-8")
    def get_ttl(self):
        return self.data[288:304].decode("utf-8")
    def get_replication(self):
        return self.data[304:312].decode("utf-8")
    def get_reserved(self):
        return self.data[312:352].decode("utf-8")
    def get_content(self):
        return self.data[352:352+self.size].decode("utf-8")

class DHTMessageGET(DHTMessageParent):
    pass

class DHTMessageGET_REPLY:
    pass

class DHTMessageTRACE:
    pass

class DHTMessageTRACE_REPLY:
    pass

class DHTMessageERROR:
    pass

msg2 = DHTMessage()
msg2.read_file('test_messages/DHTPUT')
msg2.make()

print("key is", msg2.message.get_key())
print("ttl is", msg2.message.get_ttl())
print("replication is", msg2.message.get_replication())
print("reserved is", msg2.message.get_reserved())
print("content is", msg2.message.get_content())
