#!/usr/bin/python3

# Note: Always use unittest.sh to run the tests!

import unittest
import imp
from helpers.validator import *
import datetime
from jsonschema import validate, Draft3Validator
from jsonschema.exceptions import ValidationError

class TestValidator(unittest.TestCase):

  def test_property_get(self):

        #with self.assertRaises(ValidationError):
        #    validate({"node_id":"123", "node_address": "tcp://127.0.0.1:10/1"}, SCHEMA_UPDATE_PREDECESSOR)

        #validate({"node_id":213, "node_address": "tcp://127.0.0.1:10/1"}, SCHEMA_UPDATE_PREDECESSOR)


        instance = {'successor': {'node_id': 78, 'node_address': 'tcp://127.0.0.1:1337/0'}, 'predecessor': {'node_id': 116, 'node_address': 'tcp://127.0.0.1:1338/0'}, 'node_id': 78, 'node_address': 'tcp://127.0.0.1:1337/0'}
        schema = SCHEMA_OUTGOING_RPC["rpc_get_node_info"]
        v = Draft3Validator(schema)
        errors = sorted(v.iter_errors(instance), key=lambda e: e.path)
        for error in errors:
            print(error.message)





if __name__ == '__main__':
    unittest.main()
