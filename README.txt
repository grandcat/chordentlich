Setup for GNU/Linux Systems
======================
You need to install  pip for python 3 first to install further packages.
On Debian/Ubuntu run

`apt-get install python3-pip`

on Fedora/RHEL/CentOS run

`yum install python3-pip`

now install the modules:
```
pip3 install  jsonschema
pip3 install  aiomas
```

Run nodes
======================
We provided a test script for the Gnome and XFCE desktop environment.
To start them use either

`gnome_start.sh` or
`xfce_start.sh`

You can add 3 nodes by calling

`gnome_add3.sh` or
`xfce_add3.sh`

Now you can start `dhtQuery.py` to add content to the DHT. You can store and
lookup by a integer key here. Just follow the instructions.

Run custom nodes
======================
You can run nodes with custom properties using the console. For example:

`python3 main.py -i 1333 -b 1337 -B 127.0.0.1`

runs a node on port 1333 with bootstrap node 127.0.0.1/1337
The parameters are:

-i The node port
-I The node IP address
-B Bootstrap Node IP
-b Bootstrap Node Port
-h Path to Hostkey .pem file. Used to generate custom node id
-c Path to config file. The above properties will override the config filename

Config file
======================

No Section
- HOSTKEY: Path to pem keyfile
- LOG: Additional logfile path
- PORT: Own port

Section KX
- KX: Port when using a KX Module

Section DHT
- OVERLAY_HOSTNAME: Boostrap node Id
- PORT: Port for the API
- HOSTNAME: Own IP address

Section Bootrap
- PORT: Bootstrap Node Port

Setup Mininet
======================

As the native virtual machine provided by mininet was terribly slow, we used
a XUbuntu installation within a virtual machine
(tested with Gnome Boxes on Fedora 21 and Virtual Box on Windows 10).

To test run the python script `sudo python startMininet.sh`. You may want to call
`sudo mn` and enter `quit`, if there appear any errors regarding used ports.
