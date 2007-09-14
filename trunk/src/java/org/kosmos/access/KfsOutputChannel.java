/**
 * $Id: KfsOutputChannel.java $
 *
 * Created 2007/09/11
 *
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
 * \brief An output channel that does buffered I/O.  This is to reduce
 * the overhead of JNI calls.
 */

package org.kosmos.access;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class KfsOutputChannel implements WritableByteChannel, Positionable
{
    // To get to a byte-buffer from the C++ side as a pointer, need
    // the buffer to be direct memory backed buffer.  So, allocate one
    // for reading/writing.
    private static final int DEFAULT_BUF_SIZE = 1 << 20;
    private ByteBuffer writeBuffer;
    private int kfsFd = -1;

    private final static native
    int close(int fd);

    private final static native
    int write(int fd, ByteBuffer buf, int begin, int end);

    private final static native
    int sync(int fd);

    private final static native
    int seek(int fd, long offset);

    private final static native
    long tell(int fd);

    public KfsOutputChannel(int fd) 
    {
        writeBuffer = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        writeBuffer.clear();

        kfsFd = fd;
    }

    public boolean isOpen()
    {
        return kfsFd > 0;

    }

    // Read/write from the specified fd.  The basic model is:
    // -- fill some data into a direct mapped byte buffer
    // -- send/receive to the other side (Jave->C++ or vice-versa)
    //

    public int write(ByteBuffer src) throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        int r0 = src.remaining();

        // While the src buffer has data, copy it in and flush
        while(src.hasRemaining())
        {
            if (writeBuffer.remaining() == 0) {
                writeBuffer.flip();
                writeDirect(writeBuffer);
            }

            // Save end of input buffer
            int lim = src.limit();

            // Copy in as much data we have space
            if (writeBuffer.remaining() < src.remaining())
                src.limit(src.position() + writeBuffer.remaining());
            writeBuffer.put(src);

            // restore the limit to what it was
            src.limit(lim);
        }

        int r1 = src.remaining();
        return r0 - r1;
    }

    private void writeDirect(ByteBuffer buf) throws IOException
    {
        if(!buf.isDirect())
            throw new IllegalArgumentException("need direct buffer");

        int pos = buf.position();
        int last = buf.limit();

        if (last - pos == 0)
            return;

        int sz = write(kfsFd, buf, pos, last);
        
        if(sz < 0)
            throw new IOException("writeDirect failed");

        // System.out.println("Wrote via JNI: kfsFd: " + kfsFd + " amt: " + sz);

        if (sz == last) {
            buf.clear();
            return;
        }

        if (sz == 0) {
            return;
        }

        // System.out.println("Compacting on kfsfd: " + kfsFd);

        // we wrote less than what is available.  so, shift things
        // over to reflect what was written out.
        ByteBuffer temp = ByteBuffer.allocateDirect(DEFAULT_BUF_SIZE);
        temp.put(buf);
        temp.flip();
        buf.clear();
        buf.put(temp);
    }


    public int sync() throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        // flush everything
        writeBuffer.flip();
        writeDirect(writeBuffer);

        return sync(kfsFd);
    }

    // is modeled after the seek of Java's RandomAccessFile; offset is
    // the offset from the beginning of the file.
    public int seek(long offset) throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        sync();

        return seek(kfsFd, offset);
    }

    public long tell() throws IOException
    {
        if (kfsFd < 0) 
            throw new IOException("File closed");

        return tell(kfsFd);
    }

    public void close() throws IOException
    {
        if (kfsFd < 0)
            return;

        sync();

        close(kfsFd);
        kfsFd = -1;
    }

    protected void finalize() throws Throwable
    {
        if (kfsFd < 0)
            return;
        close();
    }
    
}
