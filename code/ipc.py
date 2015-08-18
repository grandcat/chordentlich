import asyncio
import base64
import logging
import random
from helpers.messageParser import DHTMessage, DHTMessagePUT


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
            # api_message = parser.read_binary(message)
            # TEST
            api_message = parser.read_file('helpers/test_messages/DHTPUT')
            # TEST END
            asyncio.Task(self.handle_api_request(api_message))

        except Exception as e:  # TODO: refine to ParseException
            self.log.warn("API message of size %d could not be parsed.", len(message))
            self.transport.close()

    @asyncio.coroutine
    def handle_api_request(self, api_message):
        if isinstance(api_message, DHTMessagePUT):
            key = random.randint(0, 255)  # api_message.get_key()
            data = api_message.get_content()
            ttl = api_message.get_ttl()
            replication = api_message.get_replication()
            print(key)

            # Convert byte array to base64 string for JSON compatibility
            # This can be replaced if "aiomas.codecs.MsgPack" is used for peer communication
            data = base64.b64encode(data).decode('utf-8')

            dht_result = yield from self.node.put_data(key, data, ttl)
            print(dht_result)

        else:
            # Command not supported
            self.log.error("Requested command not supported.")
            self.transport.close()

    def test_generate_dht_put(self):
        buffer = bytearray(30)

        return DHTMessagePUT(buffer, len(buffer))
