Initial project report
======================
Please refer to the file docs/report1/report1.pdf


Setup
======================
apt-get install python3-pip

JSON validator
----------------
pip3 install  jsonschema
mininet
-------
As the native virtual machine provided by mininet was terribly slow, we used
a XUbuntu installation within a virtual machine
(tested with Gnome Boxes on Fedora 21 and Virtual Box on Windows 10).


pip3 install aiomas
apt-get install mininet

sudo service openvswitch-controller stop
sudo update-rc.d openvswitch-controller disable
sudo mn --topo single,4

and enter to mininet:

h1 xterm -e python3 -u main.py -I h1 &
h2 xterm -e python3 -u main.py -I h2 -B h1 &
h3 xterm -e python3 -u main.py -I h3 -B h1 &
h4 xterm -e python3 -u main.py -I h4 -B h1 &

for mininet python support you have to install the mininet utils
type
mininet/util/install.sh -fw
git clone git://github.com/mininet/mininet
