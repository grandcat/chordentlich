#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from replica import Replica

class TestIniParser(unittest.TestCase):

  def test_property_get(self):
        replica = Replica(10000)
        k1 = replica.get_key("lorem".encode(), 1)
        k2 = replica.get_key("lorem".encode(), 4)
        self.assertEqual(len(replica.get_key_list("1".encode(), 3)), 3)
        self.assertNotEqual(k1, k2) # keys do not collide hopefully at a ring size of 10000
        self.assertEqual(k1, 7393)
if __name__ == '__main__':
    unittest.main()
