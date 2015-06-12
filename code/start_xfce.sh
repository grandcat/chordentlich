#!/bin/bash
echo "Start some nodes...."
for (( p=0 ; p<3; p++ )) # open 3 different chord nodes
do
	((port=1337 + $p))
	echo "Starting node on $port"
	xfce4-terminal -x python3 dhtServer.py -p $port
	 sleep 1
done
