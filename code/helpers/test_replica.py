#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from replica import Replica

class TestIniParser(unittest.TestCase):

  def test_property_get(self):
        replica = Replica(10000)

        k1 = replica.get_key(51, 1)
        k2 = replica.get_key(23, 2)

        print ("Testing Replica Key. Example key for replicatin 2 is",k2)
        print("Key list is")
        print(replica.get_key_list(123,6))

        self.assertEqual((replica.get_key_list(8, 8)[0:3]), (replica.get_key_list(8, 4)[0:3]))

        self.assertEqual(len(replica.get_key_list(8, 3)), 3)
        self.assertNotEqual(k1, k2) # keys do not collide hopefully at a ring size of 10000
        self.assertEqual(k1, 7195)
if __name__ == '__main__':
    unittest.main()
