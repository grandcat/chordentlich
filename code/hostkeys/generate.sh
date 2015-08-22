#!/bin/bash
echo "Generating keys..."
for (( p=0 ; p<10; p++ ))
do

openssl genrsa -out  prv$p.pem  -aes256 -passout pass:foobar  4096
openssl pkey  -passin pass:foobar   -pubout -inform PEM -outform DER -in prv$p.pem -out pub$p.der
openssl dgst  -passin pass:foobar   -out pub$p.sha256 -sha256 -binary pub$p.der  #actual 32 bytes of the peer ID outputted to stdout
#openssl dgst -sha256 -hex pub$p.der  > sha256$p

done
