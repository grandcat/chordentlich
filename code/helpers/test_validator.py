#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from validator import *
import datetime
from jsonschema import validate
from jsonschema.exceptions import ValidationError

class TestValidator(unittest.TestCase):

  def test_property_get(self):

      with self.assertRaises(ValidationError):
          validate({"node_id":"123", "node_address": "tcp://127.0.0.1:10/1"}, SCHEMA_UPDATE_PREDECESSOR)

      validate({"node_id":213, "node_address": "tcp://127.0.0.1:10/1"}, SCHEMA_UPDATE_PREDECESSOR)

      #self.assertEqual(storage.get("a")[0], "a") # check if item value is correct


if __name__ == '__main__':
    unittest.main()
