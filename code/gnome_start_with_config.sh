#!/bin/bash
killall python3
sleep 0.5

count=3
startport=1338

gnome-terminal -x python3 main.py -c "config/1337.ini"

echo "Start some nodes...."
for (( p=0 ; p<$count; p++ )) # open 3 different chord nodes
do
	((port=$startport + $p))
	echo "Starting node on $port with startport $startport"
	gnome-terminal -x python3 main.py  -c "config/${port}.ini"
	sleep 0.3
done
