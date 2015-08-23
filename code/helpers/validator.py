# Check server response for outgoing RPCs
SCHEMA_OUTGOING_RPC = {}
# Check incoming RPC parameters
SCHEMA_INCOMING_RPC = {}

SCHEMA_OUTGOING_RPC["rpc_dht_put_data"] = {
    "type" : "object",
     "properties" : {
        "status" : {"type" : "number"},
        "message" : {"type" : "string"}
     },
     "required": ["status"]

}

SCHEMA_OUTGOING_RPC["rpc_dht_get_data"] = {
    "type" : "object",
     "properties" : {
        "status" : {"type" : "number"},
        "data" : {
            "type" : "array",
            "items":
                {
                    "type": "string"
                }
        }
     },
     "required": ["status"]
}

#  rpc_find_successor_rec: {'trace': [], 'node_address': 'tcp://127.0.0.1:1339/0', 'node_id': 8}
SCHEMA_OUTGOING_RPC["rpc_find_successor_rec"] = {
    "type" : "object",
    "properties" : {
        "status" : {"type" : "number"},
        "node_id" : {"type" : "number"},
        "node_address" : {"type" : "string"},
        "trace" : {
            "type" : "array",
            "items" : {
                "type" : "object",
                "properties" : {
                    "node_id" : {"type" : "number"},
                    "node_address" : {"type" : "string"},
                },
                "required": ["node_id", "node_address"]
            }
        }
    },
    "required": ["status"]
}

SCHEMA_OUTGOING_RPC["rpc_update_predecessor"] = {
    "type" : "object",
     "properties" : {
        "node_id" : {"type" : "number"},
        "node_address" : {"type" : "string"},
        "old_predecessor" : {
            "type" : "object",
            "properties" : {
                    "node_id" : {"type" : "number"},
                    "node_address" : {"type" : "string"},
                    "status" : {"type" : "number"}

            },
            "required": ["node_id", "node_address"]
        }
     }
}

SCHEMA_OUTGOING_RPC["rpc_get_node_info"] = {}

SCHEMA_INCOMING_RPC["rpc_get_node_info"] = {
     "type" : "object",
     "properties" : {

        "node_id" : {"type" : "number"},
        "node_address" : {"type" : "string",},
        "successor" :  {
            "type" : "object",
            "optional": "True",
            "properties" : {
                "node_id" : {"type" : "number"},
                "node_address" : {"type" : "string"},

            },
            "required": ["node_id", "node_address"]
        },
        "predecessor" : {
            "type" : "object",
            "optional":  "TRUE",
            "properties" : {
                "node_id" : {"type" : "number"},
                "node_address" : {"type" : "string"}
            },
            "required": ["node_id", "node_address"]
        }
     }
}

SCHEMA_OUTGOING_RPC["rpc_update_finger_table"] = {}
SCHEMA_OUTGOING_RPC["rpc_update_successor"] = {}
SCHEMA_OUTGOING_RPC["rpc_get_fingertable"] = {}

# SCHEMA_RPC[]
# Schema for the DHT messages constructed in messageParser.py
#
# Note: As the bytes do not allow greater values for integers etc, this is pretty useless at the moment.
SCHEMA_MSG_DHT = {}
SCHEMA_MSG_DHT["MSG_DHT_GET"] = {}
SCHEMA_MSG_DHT["MSG_DHT_PUT"] = {
    "type" : "object",
     "properties" : {
        "ttl" : {"type" : "number", "minimum":0, "maximum": 255},
        "replication" :  {"type" : "number", "minimum":0, "maximum": 255}
     }
}
SCHEMA_MSG_DHT["MSG_DHT_TRACE"] = {}
SCHEMA_MSG_DHT["MSG_DHT_ERROR"] = {}
