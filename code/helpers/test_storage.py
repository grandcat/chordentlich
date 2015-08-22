#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
from helpers.chordInterval import *
from helpers.storage import Storage
import datetime
import imp

class TestStorage(unittest.TestCase):

  def test_property_get(self):
      storage = Storage()

      # insert some keys a and b
      storage.put("a", "a")
      storage.put("b", "b")
      storage.put("a", "abc")

      self.assertEqual(storage.get("a")[0], "a") # check if item value is correct
      self.assertEqual(storage.get("a")[1], "abc")
      self.assertEqual(len(storage.get("a")), 2) # two elements for key a
      self.assertEqual(len(storage.data), 2) # 2 keys in total (a and b)

      longTimeAgo = datetime.datetime.today() - datetime.timedelta(2) # insert item from two days ago
      storage.put("a", "long", timeOfInsert=longTimeAgo.time().isoformat()) #insert one more item for a which is expired

      self.assertEqual(storage.get("a")[2], "long") # check if expired item was inserted
      self.assertEqual(len(storage.get("a")), 3) # check total items for key a

      storage.clean_old() # after a cleanup the expired item should be removed

      self.assertEqual(len(storage.get("a")), 2)  # check total items for key a after expired item was removed
      self.assertEqual(storage.get("b")[0] ,"b")

      storage2 = Storage()
      storage2.put(1, 1)
      storage2.put(2, 2)
      storage2.put(3, 3)
      storage2.put(4, 4)
      storage2.put(5, 5)
      storage2.put(6, 6)

      storage3 = Storage()
      storage3.put(1, 1)
      storage3.put(1, 1)
      storage3.put(1, 1)

      storage2.merge(storage3.data)
      self.assertEqual(len(storage2.get(1)) ,4)
      self.assertEqual(len (storage2.get_storage_data_between(1,4)), 3)
      self.assertEqual(len (storage2.data),6)
      storage2.delete_storage_data_between(1,4)
      self.assertEqual(len (storage2.data), 3)



if __name__ == '__main__':
    unittest.main()
