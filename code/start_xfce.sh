#!/bin/bash
killall python3
sleep 0.5

count=5
startport=1338

xfce4-terminal -H -x  python3 main.py -i 1337 -h "hostkeys/prv1.pem"

echo "Start some nodes...."
for (( p=0 ; p<$count; p++ )) # open 3 different chord nodes
do
	((port=$startport + $p))
	echo "Starting node on $port with startport $startport"
		xfce4-terminal -H -x python3 main.py -i $port -b 1337 -B 127.0.0.1 -h "hostkeys/prv${port}.pem"
	sleep 1
done
