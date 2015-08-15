#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from storage import Storage
import datetime

class TestStorage(unittest.TestCase):

  def test_property_get(self):
      storage = Storage()
      print("Testing Data Storage")
      storage.put("a", "a")
      storage.put("b", "b")
      storage.put("a", "abc")
      self.assertEqual(storage.get("a"), "abc")
      self.assertEqual(len(storage.data), 2)

      longTimeAgo = datetime.datetime.today() - datetime.timedelta(2) # insert item from two days ago
      storage.put("a", "long", longTimeAgo)
      
      print("Testing Data Storage Removal of old items")
      self.assertEqual(storage.get("a"), "long")
      storage.clean_old()
      self.assertEqual(storage.get("a"), None)
      self.assertEqual(storage.get("b"), "b")

if __name__ == '__main__':
    unittest.main()
