#!/bin/bash
killall python3
sleep 0.5

count=4
port_start=1337

echo "Start some nodes...."
for (( p=0 ; p<$count; p++ ))
do
	((port=$port_start + $p))
	echo "Starting node on $port"
	xfce4-terminal -H -x python3 main.py -p $port -c $count -s $port_start
	sleep 0.5
done
