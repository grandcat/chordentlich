#!/usr/bin/python3
import socket
from helpers.messageParser import *

print("This is a Chord interface. You can store and lookup entries here.")
port = 1337

while True:
    var = input("Do you want to (s)tore or (l)ookup?")

    if var == "s":
        key = input("Enter a key as integer: ")
        val = input("Enter a value: ")

        print("Storing value %s under %s" % (val, key));

    elif var == "l":

        key = input("Enter a key as integer:")

        if key == "":
            print("You did not provide a key. It's like entering a house. No key. No access. I am sorry.")
        else:
            frame = bytearray()

            size = 40

            frame += size.to_bytes(2, byteorder='big')
            frame += (501).to_bytes(2, byteorder='big') # 503 is MSG_DHT_GET?REPLY
            frame += int(key).to_bytes(32, byteorder='big')
            #frame += (content).to_bytes(len(content), byteorder='big')

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = ('127.0.0.1', port)

            try:
                sock.connect(server_address)

                # validate(message, schema)
                sock.sendall(bytes(json.dumps(message), 'UTF-8'))
                amount_received = 0
                data_available = 1

                output = bytearray()
                while data_available > 0 and amount_received < 1024:
                    data = sock.recv(16)
                    data_available = len(data)
                    amount_received += len(data)
                    output.extend(data)

                msg2 = DHTMessage()
                msg2.read_binary(output)
                msg3.message.get_key() # her we should get a DHT_GET_REPLY MESSAGE

            except Exception as error:
                print("Something went wrong. Make sure you can connect to your localhost node on port", port)

            finally:
                sock.close()

    else:
        print("invalid value")
