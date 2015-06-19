class validation:

    data_put = {
        "type": "object",
        "properties": {
            "action": {"type" : "string"},
            "key": {"type" : "string"},
            "value": {"type" : "string"},
        }
    }

    find_successor = {
        "type": "object",
        "properties": {
            "action": {"type" : "string"},
            "key": {"type" : "string"},
        }
    }