#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from messageParser import *

class TestMessageParser(unittest.TestCase):

  def test_property_get(self):

        msg2 = DHTMessage()
        msg2.read_file('helpers/test_messages/DHTPUT')

        print("key is", msg2.message.get_key())
        print("ttl is", msg2.message.get_ttl())
        print("replication is", msg2.message.get_replication())
        print("reserved is", msg2.message.get_reserved())
        print("content is", msg2.message.get_content())

        self.assertEqual( msg2.message.get_content().decode("utf-8"), "HALLO WELT")
        self.assertEqual(msg2.message.get_ttl(), 1)

        msg3 = DHTMessage()
        msg3.read_file('helpers/test_messages/DHTGET')

        self.assertEqual(msg3.message.get_key(), 1229782938247303441)

        msg3 = DHTMessage()
        msg3.read_file('helpers/test_messages/DHTTRACE')

        self.assertEqual(msg3.message.get_key(), 1229782938247303441)

        # Generate a new TRACE REPLY
        hops = [
            DHTHop(123, 213, "2.241.51.1", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329"),
            DHTHop(123, 213, "1.1.1.1", "FE80:0000:0000:0000:0202:B3FF:FE1E:8329")
        ]
        
        msg4 = DHTMessageTRACE_REPLY(123, hops)
        # validate key
        self.assertEqual(msg4.frame[4:36], b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00{')
        # validate first and second hop peerId
        self.assertEqual(msg4.frame[36:68], b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00{')

        self.assertEqual(msg4.frame[(36+56):(68+56)], b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00{')

if __name__ == '__main__':
    unittest.main()
