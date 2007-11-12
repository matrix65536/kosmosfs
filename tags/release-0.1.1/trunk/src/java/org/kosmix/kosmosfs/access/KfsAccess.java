/**
 * $Id$ 
 *
 * Created 2007/08/24
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright 2007 Kosmix Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * \brief Java wrappers to get to the KFS client.
 */

package org.kosmix.kosmosfs.access;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KfsAccess
{
    private final static native
    int initF(String configFn);

    private final static native
    int initS(String metaServerHost, int metaServerPort);

    private final static native
    int cd(String  path);

    private final static native
    int mkdirs(String  path);

    private final static native
    int rmdir(String  path);

    private final static native
    String[] readdir(String path);

    private final static native
    String[][] getDataLocation(String path, long start, long len);

    private final static native
    short getReplication(String path);

    private final static native
    short setReplication(String path, int numReplicas);

    private final static native
    long getModificationTime(String path);

    private final static native
    int create(String path, int numReplicas, boolean exclusive);

    private final static native
    int remove(String path);

    private final static native
    int rename(String oldpath, String newpath, boolean overwrite);

    private final static native
    int open(String path, String mode, int numReplicas);

    private final static native
    int exists(String path);

    private final static native
    int isFile(String path);

    private final static native
    int isDirectory(String path);

    private final static native
    long filesize(String path);

    static {
        try {
            System.loadLibrary("kfs_access");
        } catch (UnsatisfiedLinkError e) {
            e.printStackTrace();
            System.err.println("Unable to load kfs_access native library");
            System.exit(1);
        }
    }

    public KfsAccess(String configFn) throws IOException
    {
        int res = initF(configFn);
        if (res != 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    public KfsAccess(String metaServerHost, int metaServerPort) throws IOException
    {
        int res = initS(metaServerHost, metaServerPort);
        if (res != 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
    }

    // most calls wrap to a call on the KfsClient.  For return values,
    // see the comments in libkfsClient/KfsClient.h
    //
    public int kfs_cd(String path)
    {
        return cd(path);
    }

    // make the directory hierarchy for path
    public int kfs_mkdirs(String path)
    {
        return mkdirs(path);
    }

    // remove the directory specified by path; remove will succeed only if path is empty.
    public int kfs_rmdir(String path)
    {
        return rmdir(path);
    }

    public String[] kfs_readdir(String path)
    {
        return readdir(path);
    }

    public KfsOutputChannel kfs_append(String path)
    {
        // when you open a previously existing file, the # of replicas is ignored
        int fd = open(path, "a", 1);
        if (fd < 0)
            return null;
        return new KfsOutputChannel(fd);
    }

    public KfsOutputChannel kfs_create(String path)
    {
        return kfs_create(path, 1);
    }

    public KfsOutputChannel kfs_create(String path, int numReplicas)
    {
        return kfs_create(path, numReplicas, false);
    }

    // if exclusive is specified, then create will succeed only if the
    // doesn't already exist
    public KfsOutputChannel kfs_create(String path, int numReplicas, boolean exclusive)
    {
        int fd = create(path, numReplicas, exclusive);
        if (fd < 0)
            return null;
        return new KfsOutputChannel(fd);
    }

    public KfsInputChannel kfs_open(String path)
    {
        int fd = open(path, "r", 1);
        if (fd < 0)
            return null;
        return new KfsInputChannel(fd);
    }

    public int kfs_remove(String path)
    {
        return remove(path);
    }

    public int kfs_rename(String oldpath, String newpath)
    {
        return rename(oldpath, newpath, true);
    }

    // if overwrite is turned off, rename will succeed only if newpath
    // doesn't already exist
    public int kfs_rename(String oldpath, String newpath, boolean overwrite)
    {
        return rename(oldpath, newpath, overwrite);
    }

    public boolean kfs_exists(String path)
    {
        return exists(path) == 1;
    }

    public boolean kfs_isFile(String path)
    {
        return isFile(path) == 1;
    }

    public boolean kfs_isDirectory(String path)
    {
        return isDirectory(path) == 1;
    }
    
    public long kfs_filesize(String path)
    {
        return filesize(path);
    }

    // Given a starting byte offset and a length, return the location(s)
    // of all the chunks that cover the region.
    public String[][] kfs_getDataLocation(String path, long start, long len)
    {
        return getDataLocation(path, start, len);
    }

    // Return the degree of replication for this file
    public short kfs_getReplication(String path)
    {
        return getReplication(path);
    }

    // Request a change in the degree of replication for this file
    // Returns the value that was set by the server for this file
    public short kfs_setReplication(String path, int numReplicas)
    {
        return setReplication(path, numReplicas);
    }

    public long kfs_getModificationTime(String path)
    {
        return getModificationTime(path);
    }

    protected void finalize() throws Throwable
    {
        release();
        super.finalize();
    }

    public void release()
    {

    }

}


