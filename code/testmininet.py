#!/usr/bin/python

from subprocess import os
from mininet.net import Mininet
from mininet.topo import SingleSwitchTopo, LinearTopo
import time

os.popen("sudo mn --test pingall").read() # call this to clean up existing mininet stuff
tree4 = SingleSwitchTopo(5)
net = Mininet(topo=tree4)
net.start()

print("Starting bootstrap node on %s", net.hosts[0].IP())
net.hosts[0].cmd(' xterm -e python3 -u main.py -I %s&' % ((net.hosts[0].IP())))

for i in range(1,4):
    time.sleep(2)
    print("Make node on %s", net.hosts[i].IP())
    print (net.hosts[i].cmd(' xterm -e python3 -u main.py -I %s -B %s&' % (net.hosts[i].IP(),net.hosts[0].IP())))
    
net.stop()
