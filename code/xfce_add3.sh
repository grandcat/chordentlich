#!/bin/bash
sleep 0.5

count=3
startport=1342

echo "Start additional nodes..."
for (( p=0 ; p<$count; p++ ))
do
	((port=$startport + $p))
	echo "Starting node on $port with startport $startport"
	xfce4-terminal -H -x python3 main.py -i $port -b 1337 -B 127.0.0.1 -h "hostkeys/prv${port}.pem"
	sleep 0.3
done
