#!/bin/bash
echo "Generating keys..."
for (( p=1338 ; p<1348; p++ ))
do

openssl genrsa -out  prv$p.pem  -aes256 -passout pass:TESTCHORDDHT 4096
#openssl pkey  -passin pass:foobar   -pubout -inform PEM -outform DER -in prv1.pem
#openssl dgst  -passin pass:foobar   -out pub$p.sha256 -sha256 -binary pub$p.der  #actual 32 bytes of the peer ID outputted to stdout
#openssl dgst -sha256 -hex pub$p.der  > sha256$p


# openssl pkey  -passin pass:foobar   -pubout -inform PEM -outform DER -in prv1.pem |
done
