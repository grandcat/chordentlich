#!/usr/bin/python3

# Storage Class which is called by PUT and GET Operations
import datetime

class Storage:
    def __init__(self):
        self.data = {}

    def put(self, key, value, timeOfInsert=None):
        if not timeOfInsert:
            timeOfInsert = datetime.datetime.now()

        self.data[key] = {"value": value, "timeOfInsert": timeOfInsert}

    def get(self, key):
        if not key in self.data:
            return None
        return self.data[key]["value"]

    def timeDiffOneDay(self, timeStart, timeStop):
        diff = timeStop-timeStart
        if diff >= datetime.timedelta(days=1):
            return True
        else:
            return False

    def clean_old(self):
        keysToDelete = []
        for key in self.data:
            item = self.data[key]
            if self.timeDiffOneDay(item["timeOfInsert"], datetime.datetime.now()):
                keysToDelete.append(key)

        for key in keysToDelete:
            del (self.data[key])
