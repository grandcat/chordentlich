#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
from  Node import in_interval, CHORD_RING_SIZE

class TestNode(unittest.TestCase):

  def test_property_get(self):

      print (CHORD_RING_SIZE)
      self.assertTrue(in_interval(12, 1, 100 ))
      self.assertTrue(in_interval(12, 0, 99999999999999999999999999999 ))
      self.assertTrue(in_interval(12, 222, 0 ))
      self.assertTrue(in_interval(12, 1, 1 ))
      self.assertFalse(in_interval(12, 1111111111111111111111111111, 11111111111111111111111111111111 ))
      self.assertFalse(in_interval(12, 123, 123123 ))
      self.assertFalse(in_interval(12, 321321312, 123123 ))
      self.assertFalse(in_interval(12, 13, 19 ))

if __name__ == '__main__':
    unittest.main()
