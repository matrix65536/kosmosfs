
#
# $Id$
#
# Created on 2007/08/23
#
# Copyright 2007 Kosmix Corp.
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
# Sriram Rao
# Kosmix Corp.
# (sriram at kosmix dot com)

=================

Welcome to the Kosmos File System (KFS)!  The doc directory has files that
you should read before deploying KFS:

 - doc/COPYING : Contains the Gnu Public License information.

 - doc/INTRO.txt: Describes what KFS is about and the set of features
 that are currently implemented.

 - doc/COMPILING.txt: Describes how to compile the source code

 - doc/DEPLOYING.txt: Describes how to deploy KFS (either on a single
   node or on a cluster of machines)

 - doc/USING.txt: Contains information about to use a KFS deployment.
   In particular, how to load data into KFS from your local file
   system for the first time; the set of tools that are included for
   accessing the KFS directory tree.

 - doc/APPS_INT.txt: Describes how to integrate KFS with your
   applications.


DIRECTORY ORGANIZATION
======================
 - kfs (top-level directory)
    |
    |---> conf            (sample config files)
    |---> doc
    |---> examples        (Example client code for accessing KFS)
    |
    |---> src
           |
           |----> cc
                  |
                  |---> access          (Java/Python glue code)
                  |---> meta            (meta server code)
                  |---> ChunkServer     (chunk server code)
                  |---> libkfsClient    (client library code)
                  |---> libkfsIO        (IO library used by KFS)
                  |---> common          (common declarations)
                  |---> fuse            (FUSE module for Linux)
                  |---> tools           (KFS tools)
           |
           |----> java
                  |---> org/kosmos/access: Java wrappers to call KFS-JNI code
           |                  
           |----> python
                  |---> tests           (Python test scripts)

