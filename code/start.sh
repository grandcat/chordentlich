#!/bin/bash
killall python3
sleep 0.5

count=2
port_start=1337

echo "Start some nodes...."
for (( p=0 ; p<$count; p++ )) # open 3 different chord nodes
do
	((port=$port_start + $p))
	echo "Starting node on $port"
	gnome-terminal -x python3 Node.py -p $port -c $count -s $port_start
	sleep 0.5
done
