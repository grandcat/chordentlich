#!/usr/bin/python3

"""
The Storage class manages the data structure for *DHT_PUT* and *DHT_GET* operations.
It automatically deletes items when its timeToLive has expired and supports
multiple entrys per key.

"""

# Storage Class which is called by PUT and GET Operations
import datetime

class Storage:
    """
    Manage PUT and GET operations
    """
    def __init__(self):
        self.data = {}
    def put(self, key, value, ttl=43200,timeOfInsert=None):
        """Set default attribute values only

        :param key: the dht key
        :param value: the dht value
        :todo: Add support for binary keys and values
        :param ttl: time to live in seconds. after this period of seconds the item will be deleted
        :param timeOfInsert: datetime when the item was inserted. If none is set, we will use the current date.
        :Example: See example of  :py:meth:`get`  method.
        """
        if not timeOfInsert:
            timeOfInsert = datetime.datetime.now()

        if ttl>43200:
            raise AttributeError("TTL must be below 43200.")

        if not key in self.data: # if there does not exists a item of the given key, we create a new list
            self.data[key] = []

        self.data[key].append({"value": value, "timeOfInsert": timeOfInsert, "ttl": ttl})

    def get(self, key):
        """Returns a list of elements for the given key."

        :param key: the dht key
        :returns: list of values
        :rtype: list
        :todo: Add support for binary keys and values
        :Example:

             .. code-block:: python

                storage = Storage()
                storage.put("penguin", "tux")
                storage.put("penguin", "linus")
                storage.get("penguin") # returns ["tux", "linus"]
        """
        returnValues = []
        if not key in self.data or len(self.data[key]) == 0:
            return []
        else:
            for key2, val in enumerate(self.data[key]):
                returnValues.append( self.data[key][key2]["value"])
        return returnValues

    def timeDiff(self, timeStart, timeStop, ttl):
        diff = timeStop-timeStart
        if diff >= datetime.timedelta(seconds=ttl):
            return True
        else:
            return False

    def clean_old(self):
        """Clean old items where the time to live has expired.
        """
        for bucketKey in self.data:
            keysToDelete = []
            bucket = self.data[bucketKey]
            for key, val in enumerate(bucket):
                item = bucket[key]
                if self.timeDiff(item["timeOfInsert"], datetime.datetime.now(), item["ttl"]):
                    keysToDelete.append(key)

            for key in keysToDelete:
                del (self.data[bucketKey][key])