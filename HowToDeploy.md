### Introduction ###

The KFS package comes with scripts for installing/starting/stopping KFS servers on a set of machines. The scripts use ssh to login and execute commands (either on a single node or in a cluster).  The scripts require that the user have "no password" ssh access to every affected server. For every host affected, be sure you can "ssh ''host'' true" without being prompted for a password.

### Defining Machine Configuration for KFS ###
The system configuration for a KFS deployment can be defined using two files.  One that defines the enviroment (such as, paths, port numbers to etc) for all the chunkservers and another file that lists the nodes on which the chunkservers should be started.  The set of machines on which KFS servers have to be deployed is defined in a configuration file that follows the Python config file format. For example, the config file machines.cfg:
```
[metaserver]
node: machine1
clusterkey: kfs-test-cluster
rundir: /mnt/kfs/meta
baseport: 20000
loglevel: INFO
numservers: 2
[chunkserver_defaults]
rundir: /mnt/kfs/chunk
chunkDir: /mnt/kfs/chunk/bin/kfschunk
baseport: 30000
space: 3400 G
loglevel: INFO
```
Next, assuming that there are 3 nodes in the cluster, their names should be listed in a separate file, machines.txt.  The format of this file is one entry per line.  For example,
```
10.2.3.1
10.2.3.2
10.2.3.3
```

### Script for install/launching processes ###
Two sets of scripts are checked into the repository:
  * ''Linux'': Use ~/code/kfs/scripts
  * ''Solaris, Mac OSX'': Use ~/code/kfs/scripts.solaris

The instructions in this section are for Linux platform.  Use the corresponding script in scripts.solaris for Solaris/Mac platforms.  When you use any script which will transfer files (particularly kfssetup) on the Macintosh, you will need the additional option ''--tar=tar''; the scripts assume that GNU tar is named "gtar" otherwise.

### Installing KFS Binaries ###
Define the configuration in machines.cfg. Then, run the install script.  The install script runs a set of  processes concurrently.  This works well when the servers need to be configured for a distributed setting.  For a single node setup, the processes need to be launched sequentially:
  * When all the servers are on a single host
```
cd ~/code/kfs/scripts
python kfssetup.py -f machines.cfg -m machines.txt -b ../build -w ../webui -s
```
  * MacOSX:
```
cd ~/code/kfs/scripts.solaris
python kfssetup.py -f machines.cfg -m machines.txt -b ../build -w ../webui --tar=tar -s
```
  * When the servers are on multiple hosts
```
cd ~/code/kfs/scripts
python kfssetup.py -f machines.cfg -m machines.txt -b ../build -w ../webui
```

### Starting KFS servers ###
To launch the KFS servers, run the launch script:
```
cd ~/code/kfs/scripts
python kfslaunch.py -f machines.cfg -m machines.txt -s
```

### Checking System Status ###

To verify that the servers started up and are connected to the metaserver, use the ''kfsping'' tool:
```
cd ~/code/kfs/build/bin/tools
kfsping -m -s <metaserver host> -p <metaserver port>
```

You can also use a Web browser to monitor the servers.  The KFS package now includes a simple python-based web server that shows the set of servers that are currently connected to the metaserver.  The KFS web server runs on the **same** machine as the KFS metaserver.  The web server's port is metaserver's baseport + 50.  For example,
```
metaserver runs on machine node1 and uses port 20000
the web server runs on machine node1 at port 20050.  Point the browser at http://node1:20050
```

The KFS web server uses a file "all-machines.txt" to track where the chunkservers should be running.    This file should be manually created and placed in ~/code/kfs/webui **before** deploying KFS binaries.  The format of this file is the name of the chunkserver machines, one entry per line.  For example.
```
10.2.3.1
10.2.3.2
10.2.3.3
```
When you open the page, http://node1:20050/cluster-view, the web server will return a web page that lists:
  1. where the chunkservers are currently running
  1. where the chunkservers could not be started (due to ssh failures, etc.)
  1. where the chunkservers have failed

### Stopping KFS servers ###
To stop the KFS servers, run the launch script:
```
cd ~/code/kfs/scripts
python kfslaunch.py -f machines.cfg -m machines.txt -S
```

### Adding new chunkservers ###
It is not necessary to stop the KFS servers to add new chunkservers. Simply update the machines.cfg file and start the servers (see Starting KFS servers).

### Un-installing KFS Binaries ###
To format the filesystem/uninstall the binaries, run the install script:
```
cd ~/code/kfs/scripts
python kfssetup.py -f machines.cfg -m machines.txt -b ../build/bin -U
```