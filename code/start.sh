#!/bin/bash
killall python3
sleep 0.5

count=3
startport=1338

gnome-terminal -x python3 main.py -i 1337

echo "Start some nodes...."
for (( p=0 ; p<$count; p++ )) # open 3 different chord nodes
do
	((port=$startport + $p))
	echo "Starting node on $port with startport $startport"
	gnome-terminal -x python3 main.py -i $port -b 1337 -B 127.0.0.1
	sleep 0.3
done
