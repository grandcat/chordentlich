#!/usr/bin/python3

"""
The Storage class manages the data structure for *DHT_PUT* and *DHT_GET* operations.
It automatically deletes items when its timeToLive has expired and supports
multiple entrys per key.
"""

# Storage Class which is called by PUT and GET Operations
import datetime
from helpers.chordInterval import *

class Storage:

    """
    Manage PUT and GET operations
    """

    def __init__(self):
        self.data = {}

    def clear(self):
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
            timeOfInsert = datetime.datetime.now().isoformat()

        if ttl>43200:
            raise AttributeError("TTL must be below 43200.")

        if not key in self.data: # if there does not exists a item of the given key, we create a new list
            self.data[key] = []

        self.data[key].append({"value": value, "timeOfInsert": timeOfInsert, "ttl": ttl})

    def merge(self, dataToMerge):

        """Merge another storage to this storage

        :param dataToMerge: the dht key
        :type dataToMerge: list
        :Example:
             .. code-block:: python
                    storage2 = Storage()
                    storage2.put(1, 1)
                    storage2.put(2, 2)
                    storage3 = Storage()
                    storage3.put(1, 1)
                    storage2.merge(storage3.data)
                    # storage2.data now contains all three elements
        """

        for key in dataToMerge:
            itemsOfKey = dataToMerge[key]
            if not key in self.data: # if there does not exists a item of the given key, we create a new list
                self.data[key] = []

            for listItem in itemsOfKey:
                self.data[key].append(listItem);

    # successor must be included, predecessor must not be included
    def get_storage_data_between(self, keyOldPredecessor, keyNewPredecessor):

        """Returns a set of storage items between two nodes
        keyOldPredecessor  ===>  keyNewPredecessor  =====> This Node
        This Node send all keys from old_predecessor to new predecessor to
        the new predecessor

        :param keyOldPredecessor: the key of the old predecessor
        :param keyNewPredecessor: the key of the new predecessor
        :returns: list of data storage items
        :rtype: list
        """

        newset = {}
        for key in self.data:
            item = self.data[key]
            if in_interval(key, keyOldPredecessor, keyNewPredecessor, inclusive_right=True):
                newset[key] = item

        return newset

    def delete_storage_data_between(self, keyOldPredecessor, keyNewPredecessor):

        """Delete a set of storage items between two nodes
        Ususally called if a node has sent some of its content to another node

        :param keyOldPredecessor: the key of the old predecessor
        :param keyNewPredecessor: the key of the new predecessor
        """

        keysToDelete = []
        for key in self.data:
            if in_interval(key, keyOldPredecessor, keyNewPredecessor, inclusive_right=True):
                keysToDelete.append(key)

        for key in keysToDelete:
            del(self.data[key])

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

        """Checks if an item inserted at timeStart with a given ttl is still valid
        at timestop (ttl has not expired at timeStop)

        :param timeStart:  time when the item was inserted
        :type: timeStart: datetime
        :param timeStop: time when the item is checked
        :type: timeStop: datetime
        :param ttl: time to live in seconds
        :type ttl: int
        :returns: True if valid or false if invalid
        :rtype: Boolean
        """

        diff = timeStop-timeStart
        if diff >= datetime.timedelta(seconds=ttl):
            return True
        else:
            return False

    def clean_old(self):

        """
        Clean old items where the time to live has expired.
        """

        for bucketKey in self.data:
            keysToDelete = []
            bucket = self.data[bucketKey]
            for key, val in enumerate(bucket):
                item = bucket[key]
                timeobj = datetime.datetime.strptime(item["timeOfInsert"], "%Y-%m-%dT%H:%M:%S.%f")
                if self.timeDiff(timeobj, datetime.datetime.now(), item["ttl"]):
                    keysToDelete.append(key)

            for key in keysToDelete:
                del (self.data[bucketKey][key])
