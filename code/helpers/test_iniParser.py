#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from helpers.iniParser import IniParser

class TestIniParser(unittest.TestCase):

  def test_property_get(self):
      inip = IniParser("configExample.ini")
      self.assertEqual(inip.get("PORT", "DHT"), '4424')
      self.assertEqual(inip.get("HOSTNAME", "DHT"), "127.0.0.1")

if __name__ == '__main__':
    unittest.main()
