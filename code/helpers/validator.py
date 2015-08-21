SCHEMA_TEST = {
    "type" : "object",
     "properties" : {
        "price" : {"type" : "number"},
         "name" : {"type" : "string"},
     },
}

SCHEMA_UPDATE_PREDECESSOR = {
    "type" : "object",
     "properties" : {
        "node_id" : {"type" : "number"},
        "node_address" : {"type" : "string"},
     },
}

SCHEMA_RPC_PUT_DATA = {
    "type" : "object",
     "properties" : {
        "data" : {"type" : "string"},
     },
}



SCHEMA_RPC_UPDATE_FINGERTABLE = {


}

# Schema for the DHT messages constructed in messageParser.py
#
# Note: As the bytes do not allow greater values for integers etc, this is pretty useless at the moment.
SCHEMA_MSG_DHT = {}
SCHEMA_MSG_DHT["MSG_DHT_GET"] = {}
SCHEMA_MSG_DHT["MSG_DHT_PUT"] = {
    "type" : "object",
     "properties" : {
        "ttl" : {"type" : "number", "minimum":0, "maximum": 1},
        "replication" :  {"type" : "number", "minimum":0, "maximum": 255}
     }
}
SCHEMA_MSG_DHT["MSG_DHT_TRACE"] = {}
SCHEMA_MSG_DHT["MSG_DHT_ERROR"] = {}
