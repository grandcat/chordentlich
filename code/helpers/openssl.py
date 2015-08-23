#!/usr/bin/python3

#
# open ssl wrapper module
#
import subprocess
import os

def makeSha256FromPem(inputFile, passphrase="TESTCHORDDHT"):
    pipe = subprocess.Popen("openssl pkey -pubout  -passin pass:"+passphrase+"  -inform PEM -outform DER -in "+inputFile+" -out temp.der | openssl dgst -sha256 -hex temp.der  | sed 's/^.* //'", shell=True, stdout=subprocess.PIPE).stdout
    output = pipe.read().rstrip().decode("utf-8")
    try:
        os.remove("temp.der")
    except:
        pass

    print("OPENSSL OUTPUT IS ", output)
    
    if len(output) != 64:
        return None
    else:
        return  int(str(output), 16)  # convert to int
