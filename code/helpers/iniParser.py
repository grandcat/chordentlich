#!/usr/bin/python3

# Please note that there is also a python module named configparser. However,
# as the example ini module contanined no section header for the first
# entry (HOSTKEY), we wrote our own
#

#
# Function to validate the configuration fingertable
# This may be useful if a config file for an old version
# is intended to be used with a newer version of the application
#

class IniParser:
    def __init__(self, filename):
        self.data = {}
        self.readFile(filename)

    def readFile(self, filename):

        currentsection = "" # The current seciotn in the parser (like [DHT] for example)
        self.data = {}
        self.data[currentsection] = {}

        with open(filename) as f:
            for line in f:
                if line != "":
                    if line.startswith('['):
                        currentsection = line.strip()[1:-1]

                        self.data[currentsection] = {}
                    else:
                        ar = line.split('=', 1 )
                        self.data[currentsection][ar[0].strip()] = ar[1].strip()

    def get(self, attribute, section=""):
        return self.data[section][attribute]

    def validateConfig(self):
        pass # TODO: check config file if all required attributes are contained
