Initial project report
======================
Please refer to the file docs/report1/report1.pdf


Setup Mininet
======================

As the native virtual machine provided by mininet was terribly slow, we used
a XUbuntu installation within a virtual machine
(tested with Gnome Boxes on Fedora 21 and Virtual Box on Windows 10).

apt-get install python3-pip
pip3 install aiomas
apt-get install mininet

sudo service openvswitch-controller stop
sudo update-rc.d openvswitch-controller disable
sudo mn
> h1 python3 main.py -I h1 &
> h2 python3 main.py -I h2 -B h1
