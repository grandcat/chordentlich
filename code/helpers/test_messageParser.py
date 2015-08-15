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

        self.assertEqual( msg2.message.get_content(), "HELLO WORLD")
        self.assertEqual(msg2.message.get_ttl(), 9)

        msg3 = DHTMessage()
        msg3.read_file('helpers/test_messages/DHTGET')

        self.assertEqual(msg2.message.get_key(), "11111111111111111111111111111111")




if __name__ == '__main__':
    unittest.main()
