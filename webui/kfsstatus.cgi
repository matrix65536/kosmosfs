#!/usr/bin/env python
#
# $Id:$
#
# Copyright 2008 Quantcast Corp.
#
# Author: Sriram Rao (Quantcast Corp.)
#
# This file is part of Kosmos File System (KFS).
#
# Licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# This CGI script outputs a web page that describes system state.  To use:
# 1. Change the metaserver location for your setup
# 2. Update /usr/bin/env python --- to point python in your setup
# 3. Copy this file in the cgi-bin directory of your web server
#

import os,sys,os.path,getopt
import socket,threading,calendar
from ConfigParser import ConfigParser

upServers = {}
downServers = {}

class ServerLocation():
    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.status = 0

class UpServer:
    """Keep track of an up server state"""
    def __init__(self, info):
        serverInfo = info.split(',')
        # order here is host, port, total, used, util, nblocks, last heard
        self.host = serverInfo[0].split('=')[1].strip()
        self.port = serverInfo[1].split('=')[1].strip()
        self.total = serverInfo[2].split('=')[1].strip()
        self.used = serverInfo[3].split('=')[1].strip()
        self.util = serverInfo[4].split('=')[1].strip()
        self.nblocks = serverInfo[5].split('=')[1].strip()
        self.lastheard = serverInfo[6].split('=')[1].strip()
        
    def __cmp__(self, other):
        """ Order by hostname"""
        return cmp(self.host, other.host)

    def printHtml(self):
        print '''<tr><td align="center">''', self.host, '''</td>'''
        print '''<td>''', self.total, '''</td>'''
        print '''<td>''', self.used, '''</td>'''
        print '''<td>''', self.util, '''</td>'''
        print '''<td align="right">''', self.nblocks, '''</td>'''
        print '''<td align="right">''', self.lastheard, '''</td>'''                        
        print '''</tr>'''
        

class DownServer:
    """Keep track of a down server"""
    def __init__(self, info):
        print "Downserver: ", info
        serverInfo = info.split(',')        
        self.host = serverInfo[0].split('=')[1].strip()
        self.port = serverInfo[1].split('=')[1].strip()        
        self.downdate = serverInfo[2].split('=')[1].strip()

    def __cmp__(self, other):
        """Order by down date"""
        d1 = calendar.timegm(self.downdate)
        d2 = calendar.timegm(other.downdate)
        return cmp(d2, d1)

    def printHtml(self):
        print '''<tr><td align="center">''', self.host, '''</td>'''
        print '''<td>''', self.downdate, '''</td>'''        
        print '''</tr>'''
        
    
def processUpNodes(nodes):
    global upServers
    servers = nodes.split('\t')
    upServers = [UpServer(c) for c in servers]
    upServers.sort()

def processDownNodes(nodes):
    global downServers    
    servers = nodes.split('\t')
    if servers != "":
        downServers = [DownServer(c) for c in servers]
        downServers.sort()
        
def ping(metaserver):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((metaserver.node, metaserver.port))
    req = "PING\r\nVersion: KFS/1.0\r\nCseq: 1\r\n\r\n"
    sock.send(req)
    sockIn = sock.makefile('r')
    for line in sockIn:
        if line.find('Down Servers:') == 0:
            if (len(line.split(':')) < 2):
                break
            pos = line.find(':')
            downNodes = line[pos+1:].strip()
            processDownNodes(downNodes)
            break
        if line.find('Servers:') != 0:
            continue
        nodes = line.split(':')[1].strip()
        processUpNodes(nodes)        

    sock.close()    


def outputhtml(metaserver):
    rows = ''

    print '''content-type: text/html\r\n\r\n'''
    print '''
    <html>
    <body>
    <H1> System status </H1>
    <ul>
    <li> Alive nodes:''', len(upServers), '''
    </li>
    <li> Dead nodes:''', len(downServers), '''
    </li>
    </ul>
    <H2> Alive Nodes </H2>
    <table border=1>
    <tr><th> Chunkserver </th> <th> Space </th> <th> Used </th> <th> Util </th> <th> # of blocks </th> <th> Last heard </th></tr>
    '''
    for v in upServers:
        v.printHtml()
    print '''
    </table>
    <br>
    '''

    if len(downServers) > 0:
        print '''
        <H2> Dead Nodes </H2>
        <table border=1>
        <tr><th> Chunkserver </th> <th> Down Since </th> </tr>
        '''
        for v in downServers:
            v.printHtml()
        print '''
        </table>

    </body>
    </html>'''
    
if __name__ == '__main__':
    """To use, change the metaserver location appropriately"""
    metaserver = ServerLocation(node='localhost',
                                port=20000)
    ping(metaserver)
    outputhtml(metaserver)
