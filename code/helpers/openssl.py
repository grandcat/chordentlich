#!/usr/bin/python3

#
# open ssl wrapper module
#
import subprocess
import os
import time

def makeSha256FromPem(inputFile, passphrase="TESTCHORDDHT"):
    #time.sleep(0.1)
    pipe = subprocess.Popen("openssl pkey -pubout  -passin pass:"+passphrase+"  -inform PEM -outform DER -in "+inputFile+" -out "+inputFile+"temp.der | openssl dgst -sha256 -hex  "+inputFile+"temp.der  | sed 's/^.* //'", shell=True, close_fds=True, stdout=subprocess.PIPE).stdout
    output = pipe.read().rstrip().decode("utf-8")

    #try:
    #    os.remove("temp.der")
    #except:
    #    pass

    if len(output) != 64:
        print("WARNING: OPENSSL OUTPUT IS: ", output)
        # echo the same without regex to get full error message
        pipe = subprocess.Popen("openssl pkey -pubout  -passin pass:"+passphrase+"  -inform PEM -outform DER -in "+inputFile+" -out  "+inputFile+"temp.der | openssl dgst -sha256 -hex  "+inputFile+"temp.der", shell=True, close_fds=True,  stdout=subprocess.PIPE).stdout
        output = pipe.read().rstrip().decode("utf-8")
        print(output)

        return None
    else:
        return  int(str(output), 16)  # convert to int
