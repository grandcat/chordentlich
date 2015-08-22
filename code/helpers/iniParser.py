#!/usr/bin/python3

"""
Helper class to parse the ini files.

Please note that there is also a python module named configparser. However,
as the example ini module contanined no section header for the first
entry (HOSTKEY), we wrote our own
"""

class IniParser:
    """
    Initializes a DHT_TRACE_REPLY message to send later.

    :param filename: The filename. Example: "config.ini"
    :type filename: string
    """
    def __init__(self, filename):
        self.data = {}
        self.read_file(filename)

    def read_file(self, filename):

        currentsection = "" # The current seciotn in the parser (like [DHT] for example)
        self.data = {}
        self.data[currentsection] = {}

        with open(filename) as f:
            for line in f:
                if line.strip()!="":
                    try:
                        if line != "":
                            if line.startswith('['):
                                currentsection = line.strip()[1:-1]
                                self.data[currentsection] = {}
                            else:
                                ar = line.split('=', 1 )
                                self.data[currentsection][ar[0].strip()] = ar[1].strip()
                    except:
                        print("NOTE: Could not parse line:", line)

    def get(self, attribute, section=""):
        """
        get an attribute of an ini file

        :param attribute: The attribute
        :param section: The section. Default is no section
        :type attribute: string
        :Example:

             .. code-block:: python

                #test.ini:
                #[DHT]
                #PORT = 123
                inip = IniParser("test.ini")
                inip.get("PORT", "DHT") # returns  123
        """
        if attribute in  self.data[section]:
            return self.data[section][attribute]
        else:
            return None
