
Kosmos File System (KFS).

Created on 2007/08/23

Copyright (C) 2007 Kosmix Corp.

This file is part of Kosmix File System (KFS).

KFS is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation under version 3 of the License.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.


Sriram Rao
Kosmix Corp.
(sriram at kosmix dot com)

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

