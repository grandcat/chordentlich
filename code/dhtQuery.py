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

ipaddress = input("Enter an IPAddress (Press enter to use localhost): ")
port =  input("Enter a port (Press enter to use 4423): ")

if (port == ""):
    port = 4423
else:
    port = int(port)
    
if (ipaddress==""):
    ipaddress = "127.0.0.1"

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
            str =  val.encode("utf-8")
            frame = MAKE_MSG_DHT_PUT(key, str).get_data()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = (ipaddress, port)

            try:
                sock.connect(server_address)
                print("Send to DHT: %s" % frame)
                sock.sendall(frame)
                #amount_received = 0
                #data_available = 1

                # output = bytearray()
                # while data_available > 0 and amount_received < 1024:
                #     data = sock.recv(16)
                #     data_available = len(data)
                #     amount_received += len(data)
                #     output.extend(data)

                # Parse a DHT_GET_REPLY.
                #size = int.from_bytes(output[0:2], byteorder='big')
                # content = output[34:size]

                print("Sent STORE command for content: ",val)

            except Exception as error:
                print("Something went wrong. Make sure you can connect to your localhost node on port", port)
                print(error)

            finally:
                sock.close()



    elif var == "l":

        key = input("Enter a key as integer:")

        if key == "":
            print("You did not provide a key. It's like entering a house. No key. No access. I am sorry.")
        else:
            frame = MAKE_MSG_DHT_GET(key).get_data()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = (ipaddress, port)

            try:
                sock.connect(server_address)
                sock.sendall(frame)
                amount_received = 0
                data_available = 1

                output = bytearray()
                while data_available > 0 and amount_received < 1024:
                    data = sock.recv(16)
                    data_available = len(data)
                    amount_received += len(data)
                    output.extend(data)

                offset = 0
                while amount_received>0:
                    size = int.from_bytes(output[offset+0:offset+2], byteorder='big')
                    content = output[offset+36:offset+size].decode("utf-8")
                    amount_received-= size
                    offset +=size
                    print("Returned content is:",content)

            #except Exception as error:
            #    print (error)
            #s    print("Something went wrong. Make sure you can connect to your localhost node on port", port)

            finally:
                sock.close()

    else:
        print("invalid value")
