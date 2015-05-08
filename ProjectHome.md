Kosmos distributed file system (KFS) provides high performance combined with availability and reliability. It is intended to be used as the backend storage infrastructure for data intensive apps such as, search engines, data mining, grid computing etc.

KFS has been deployed in production settings on large clusters to manage multiple petabytes of storage.

KFS is implemented in C++ using standard system components such as STL, boost libraries, aio, log4cpp.

KFS is integrated with Hadoop and Hypertable.

KFS source code is released under the terms of the Apache License Version 2.0.

**Quantcast has now released their filesystem [QFS](http://github.com/quantcast/qfs) (which is based on the KFS code base).   Going forward, any new releases will come from their branch.**