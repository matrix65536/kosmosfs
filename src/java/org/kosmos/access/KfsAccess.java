/**
 * $Id: KfsAccess.java $
 *
 * Created 2007/08/24
 * @author: Sriram Rao (Kosmix Corp.)
 *
 * Copyright (C) 2007 Kosmix Corp.
 *
 * This file is part of Kosmix File System (KFS).
 *
 * KFS is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation under version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 * 
 * \brief Java wrappers to get to the KFS client.
 */

package org.kosmos.access;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KfsAccess
{
    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    private static final int DEFAULT_BUF_SIZE = 1 << 20;
    private ByteBuffer readBuffer;
    private ByteBuffer writeBuffer;

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
    int create(String path, int numReplicas);

    private final static native
    int remove(String path);

    private final static native
    int rename(String oldpath, String newpath, boolean overwrite);

    private final static native
    int open(String path, String mode, int numReplicas);

    private final static native
    int close(int fd);

    private final static native
    int read(int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int write(int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int sync(int fd);

    private final static native
    int seek(int fd, long offset);

    private final static native
    long tell(int fd);

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
        allocBuffers();
    }

    public KfsAccess(String metaServerHost, int metaServerPort) throws IOException
    {
        int res = initS(metaServerHost, metaServerPort);
        if (res != 0) {
            throw new IOException("Unable to initialize KFS Client");
        }
        allocBuffers();
    }

    private void allocBuffers()
    {
        readBuffer = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        writeBuffer = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        readBuffer.flip();
        writeBuffer.clear();
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

    public int kfs_create(String path)
    {
        return kfs_create(path, 1);
    }

    public int kfs_create(String path, int numReplicas)
    {
        return create(path, numReplicas);
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

    // mode is "r" for reading and "w" for writing, "rw" for read/write.
    // "a" for writing in append mode;

    public int kfs_open(String path, String mode)
    {
        return open(path, mode, 1);
    }

    public int kfs_open(String path, String mode, int numReplicas)
    {
        return open(path, mode, numReplicas);
    }

    public int kfs_close(int fd)
    {
        return close(fd);
    }

    public int kfs_sync(int fd)
    {
        return sync(fd);
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    public int kfs_seek(int fd, long offset)
    {
        return seek(fd, offset);
    }

    public long kfs_tell(int fd)
    {
        return tell(fd);
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

    protected void finalize() throws Throwable
    {
        release();
        super.finalize();
    }

    public void release()
    {

    }

    // Read/write from the specified fd.  The basic model is:
    // -- fill some data into a direct mapped byte buffer
    // -- send/receive to the other side (Jave->C++ or vice-versa)
    //
    public int kfs_read(int fd, byte[] dst, int off, int len) throws IOException
    {
        int nread = 0, todo, res;

        while (nread < len) {
            readBuffer.clear();

            int pos = readBuffer.position();
            int limit = readBuffer.limit();

            todo = Math.min(len - nread, limit);
            res = read(fd, readBuffer, pos, todo);
            if (res == 0)
                break;

            if (res < 0)
                throw new IOException("read failed");
            readBuffer.position(pos + res);
            readBuffer.flip();

            readBuffer.get(dst, off + nread, res);
            nread += res;
        }
        return nread;
    }

    public int kfs_write(int fd, byte[] src, int off, int len) throws IOException
    {
        int nwrote = 0, todo, res;

        while (nwrote < len) {
            writeBuffer.clear();

            int limit = writeBuffer.limit();

            todo = Math.min(len - nwrote, limit);
            writeBuffer.put(src, off + nwrote, todo);
            writeBuffer.flip();

            int pos = writeBuffer.position();
            limit = writeBuffer.limit();
            
            res = write(fd, writeBuffer, pos, limit);
            if (res < 0)
                throw new IOException("read failed");
            nwrote += res;
        }
        return nwrote;
    }
}


