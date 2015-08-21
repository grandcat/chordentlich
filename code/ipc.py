import asyncio
import base64
import logging
import random
from helpers.messageParser import *
from helpers.aiomasTools import aiomas_parse_url
from helpers.messageDefinitions import *

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
            if len(message) < 2: # 1 byte messages are control messages used for testing
                asyncio.Task(self.route_api_testmessage(message))
            else:
                # TEST
                # cmd = message.decode().rstrip()
                # if cmd.isdigit():
                #     api_message = parser.read_file('helpers/test_messages/DHTGET')
                #     self.get_id = int(cmd)
                # else:
                #     api_message = parser.read_file('helpers/test_messages/DHTPUT')
                # TEST END
                api_message = parser.read_binary(message)
                asyncio.Task(self.route_api_request(api_message))

        except Exception as e:  # TODO: refine to ParseException
            self.log.warn("API message of size %d could not be parsed.", len(message))
            self.transport.close()

    @asyncio.coroutine
    def route_api_testmessage(self, message):
        result = yield from self.node.test_stresstest(message)
        self.transport.write(result)    
        self.transport.close()

    @asyncio.coroutine
    def route_api_request(self, api_message):
        if isinstance(api_message, DHTMessagePUT):
            yield from self.handle_dht_put(api_message)

        elif isinstance(api_message, DHTMessageGET):
            yield from self.handle_dht_get(api_message)

        elif isinstance(api_message, DHTMessageTRACE):
            yield from self.handle_dht_trace(api_message)

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
        # TEST TRACE
        yield from self.handle_dht_trace(api_message)
        return

        key = api_message.get_key()

        dht_result = yield from self.node.get_data(key)
        # TODO: Convert base64 back to bytes
        for item in dht_result["data"]:
            data = base64.b64decode(item.encode())
            print("DHT GET result: %s" % data)

            reply = DHTMessageGET_REPLY(key, data)
            print("array for transport is: ", reply.get_data())
            self.transport.write(reply.get_data())
        self.transport.write_eof()

    @asyncio.coroutine
    def handle_dht_trace(self, api_message):
        # assert isinstance(api_message, DHTMessageTRACE)
        key = api_message.get_key()
        dht_result = yield from self.node.get_trace(key)

        hops = []
        for peer in dht_result:
            node_id = peer["node_id"]
            kx_port = 0

            host = "0.0.0.0"
            try:
                (host, port), _ = aiomas_parse_url(peer["node_address"])
            except ValueError as e:
                print("[Warn:handle_dht_trace] Could not parse hop '%s'." % peer["node_address"])
            ipv4 = host

            hop = DHTHop(node_id, kx_port, ipv4, "::")
            hops.append(hop)

        reply = MAKE_MSG_DHT_TRACE_REPLY(key, hops)
        print("Trace binary:", reply.get_data())
        self.transport.write(reply.get_data())
        self.transport.close()

    def test_generate_dht_put(self):
        buffer = bytearray(30)

        return DHTMessagePUT(buffer, len(buffer))
