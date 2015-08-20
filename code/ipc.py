import asyncio
import base64
import logging
import random
from helpers.messageParser import DHTMessage, DHTMessagePUT, DHTMessageGET, DHTMessageGET_REPLY


class ApiServer(asyncio.Protocol):
    def __init__(self, dht_node):
        self.log = logging.getLogger(__name__)
        self.node = dht_node

        self.log.info("API server listening.")

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, message):
        """
        Parses and executes API calls received from an external client.
        A dedicated asyncio task will be spawned for each incoming request.

        :param message: raw message received.
        """
        print(message)
        parser = DHTMessage()

        try:
            api_message = parser.read_binary(message)
            # TEST
            # cmd = message.decode().rstrip()
            # if cmd.isdigit():
            #     api_message = parser.read_file('helpers/test_messages/DHTGET')
            #     self.get_id = int(cmd)
            # else:
            #     api_message = parser.read_file('helpers/test_messages/DHTPUT')
            # TEST END
            asyncio.Task(self.route_api_request(api_message))

        except Exception as e:  # TODO: refine to ParseException
            self.log.warn("API message of size %d could not be parsed.", len(message))
            self.transport.close()

    @asyncio.coroutine
    def route_api_request(self, api_message):
        if isinstance(api_message, DHTMessagePUT):
            yield from self.handle_dht_put(api_message)

        elif isinstance(api_message, DHTMessageGET):
            yield from self.handle_dht_get(api_message)

        else:
            # Command not supported
            self.log.error("Requested command not supported.")
            self.transport.close()

    @asyncio.coroutine
    def handle_dht_put(self, api_message):
        assert isinstance(api_message, DHTMessagePUT)

        key = api_message.get_key()
        print("DHT PUT key: %d" % key)
        data = api_message.get_content()
        ttl = api_message.get_ttl()
        replication = api_message.get_replication()

        # Convert byte array to base64 string for JSON compatibility
        # This can be replaced if "aiomas.codecs.MsgPack" is used for peer communication
        data = base64.b64encode(data).decode('utf-8')
        dht_result = yield from self.node.put_data(key, data, ttl, replication)
        print("DHT PUT result: %s" % dht_result)

    @asyncio.coroutine
    def handle_dht_get(self, api_message):
        assert isinstance(api_message, DHTMessageGET)

        key = api_message.get_key()
        print("DHT GET key: %d" % key)
        print("ßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßßß")
        print("DHT GET key: %d" % key)
        print("DHT GET key: %d" % key)

        dht_result = yield from self.node.get_data(key)
        # TODO: Convert base64 back to bytes
        for item in dht_result["data"]:
            data = base64.b64decode(item.encode())
            print("DHT GET result: %s" % data)

            reply = DHTMessageGET_REPLY(key, data)
            print("array for transport is: ", reply.get_data())
            self.transport.write(reply.get_data())
        self.transport.write_eof()

    def test_generate_dht_put(self):
        buffer = bytearray(30)

        return DHTMessagePUT(buffer, len(buffer))
