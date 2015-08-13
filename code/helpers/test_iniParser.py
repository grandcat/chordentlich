#!/usr/bin/python3

# Note: Always use run.sh to run the tests!

import unittest
import imp
from iniParser import IniParser

class TestIniParser(unittest.TestCase):

  def test_property_get(self):
      inip = IniParser("test.ini")

      self.assertEqual(inip.get("HOSTKEY"), '/hostkey.pem')
      self.assertEqual(inip.get("PORT", "DHT"), '3086')

if __name__ == '__main__':
    unittest.main()
