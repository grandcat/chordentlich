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

      # insert some keys a and b
      storage.put("a", "a")
      storage.put("b", "b")
      storage.put("a", "abc")

      self.assertEqual(storage.get("a")[0], "a") # check if item value is correct
      self.assertEqual(storage.get("a")[1], "abc")
      self.assertEqual(len(storage.get("a")), 2) # two elements for key a
      self.assertEqual(len(storage.data), 2) # 2 keys in total (a and b)

      longTimeAgo = datetime.datetime.today() - datetime.timedelta(2) # insert item from two days ago

      print("Testing Data Storage Removal of old items")
      storage.put("a", "long", timeOfInsert=longTimeAgo) #insert one more item for a which is expired

      self.assertEqual(storage.get("a")[2], "long") # check if expired item was inserted
      self.assertEqual(len(storage.get("a")), 3) # check total items for key a

      storage.clean_old() # after a cleanup the expired item should be removed

      self.assertEqual(len(storage.get("a")), 2)  # check total items for key a after expired item was removed
      self.assertEqual(storage.get("b")[0] ,"b")

if __name__ == '__main__':
    unittest.main()
