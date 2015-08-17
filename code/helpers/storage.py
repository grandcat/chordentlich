#!/usr/bin/python3

# Storage Class which is called by PUT and GET Operations
import datetime

class Storage:
    def __init__(self):
        self.data = {}

    def put(self, key, value, ttl=43200,timeOfInsert=None):

        if not timeOfInsert:
            timeOfInsert = datetime.datetime.now()

        if ttl>43200:
            raise AttributeError("TTL must be below 43200.")

        if not key in self.data: # if there does not exists a item of the given key, we create a new list
            self.data[key] = []

        self.data[key].append({"value": value, "timeOfInsert": timeOfInsert, "ttl": ttl})

    def get(self, key):
        returnValues = []
        if not key in self.data or len(self.data[key]) == 0:
            return None
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

        for bucketKey in self.data:
            keysToDelete = []
            bucket = self.data[bucketKey]
            for key, val in enumerate(bucket):
                item = bucket[key]
                if self.timeDiff(item["timeOfInsert"], datetime.datetime.now(), item["ttl"]):
                    keysToDelete.append(key)

            for key in keysToDelete:
                del (self.data[bucketKey][key])
