#!/usr/bin/python

import time
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel

def test():
    hosts = []

    net = Mininet()
    s0 = net.addSwitch('s0')
    for id in range(0, 5):
        host = net.addHost('h%s' % id)
        net.addLink(host, s0)
        hosts.append(host)

    c0 = net.addController('c0')

    net.start()

    hosts[0].cmd('xterm -e python3 -u main.py -I %s &' % hosts[0].IP())

    for host in hosts[1:]:
        time.sleep(1)
        host.cmd('xterm -e python3 -u main.py -I %s -B %s &' % (host.IP(), hosts[0].IP()))

if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    test()