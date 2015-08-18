#!/usr/bin/python3
import socket
from helpers.messageParser import *

"""
This is a console tool made to store and get data from the DHT.
You can start and use it with ./dhtQuery.py
Please note code quality of this file is not very good, as it is only used for
simulating the KX interface.
"""

print("This is a Chord interface. You can store and lookup entries here.")
port = 4423

while True:
    var = input("Do you want to (s)tore or (l)ookup?")

    if var == "s":
        key = input("Enter a key as integer: ")
        val = input("Enter a value: ")

        print("Going to store value %s under %s..." % (val, key));

        exception = False
        try:
            int(key)
        except ValueError:
            exception = True

        if exception:
            print("ERROR: key is no integer.")
        elif key == "" or val == "":
            print("Key or value missing. Aborting...")
        else:
            frame = bytearray()
            str =  val.encode("utf-8")

            size = 22+len(str)

            frame += size.to_bytes(2, byteorder='big')
            frame += (500).to_bytes(2, byteorder='big') # 500 is MSG_DHT_GET_REPLY
            frame += int(key).to_bytes(32, byteorder='big')
            frame += int(40000).to_bytes(2, byteorder='big')
            frame += int(2).to_bytes(1, byteorder='big') # replication
            frame += int(0).to_bytes(1, byteorder='big') # reserved
            frame += int(0).to_bytes(4, byteorder='big') # reserved
            frame += len(str) # content


            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = ('127.0.0.1', port)

            try:
                sock.connect(server_address)
                sock.sendall(bytes(json.dumps(message), 'UTF-8'))
                amount_received = 0
                data_available = 1

                output = bytearray()
                while data_available > 0 and amount_received < 1024:
                    data = sock.recv(16)
                    data_available = len(data)
                    amount_received += len(data)
                    output.extend(data)

                size = int.from_bytes( self.data[0:2], byteorder='big')
                content = message[34:size]
                print("Sent STORE command for content: ",content)

            except Exception as error:
                print("Something went wrong. Make sure you can connect to your localhost node on port", port)

            finally:
                sock.close()



    elif var == "l":

        key = input("Enter a key as integer:")

        if key == "":
            print("You did not provide a key. It's like entering a house. No key. No access. I am sorry.")
        else:
            frame = bytearray()
            size = 40

            frame += size.to_bytes(2, byteorder='big')
            frame += (501).to_bytes(2, byteorder='big') # 503 is MSG_DHT_GET_REPLY
            frame += int(key).to_bytes(32, byteorder='big')

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = ('127.0.0.1', port)

            try:
                sock.connect(server_address)
                sock.sendall(bytes(json.dumps(message), 'UTF-8'))
                amount_received = 0
                data_available = 1

                output = bytearray()
                while data_available > 0 and amount_received < 1024:
                    data = sock.recv(16)
                    data_available = len(data)
                    amount_received += len(data)
                    output.extend(data)

                size = int.from_bytes( self.data[0:2], byteorder='big')
                content = message[34:size].decode("utf-8")
                print("Returned content is:",content)

            except Exception as error:
                print("Something went wrong. Make sure you can connect to your localhost node on port", port)

            finally:
                sock.close()

    else:
        print("invalid value")
