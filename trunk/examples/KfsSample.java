/**
 * $Id: KfsSample.java $
 *
 * Created 2007/08/25
 * Author: Sriram Rao
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
 * \brief A Sample Java program to access KFSAccess APIs.  To run this
 * program, you need:
 *  - kfs.jar in your CLASSPATH
 *  - libkfs_access.so in your LD_LIBRARY_PATH
 *  - a KFS deployment
 */


import java.io.*;
import java.net.*;
import java.util.Random;

public class KfsSample
{
    public static void main(String args[]) {
        if (args.length < 1) {
            System.out.println("Usage: KfsTest <meta server> <port>");
            System.exit(1);
        }
        try {
            // From the command line, get the location of the meta-server
            int port = Integer.parseInt(args[1].trim());

            // Initialize a KfsAccess object.  The KfsAccess object is
            // the glue code that gets us into KFS world.
            //
            KfsAccess kfsAccess = new KfsAccess(args[0], port);
            
            String basedir = new String("jtest");

            // Make a test directory where we can do something
            if (!kfsAccess.kfs_exists(basedir)) {
                if (kfsAccess.kfs_mkdirs(basedir) != 0) {
                    System.out.println("Unable to mkdir");
                    System.exit(1);
                }
            }

            // What we just created better be a directory
            if (!kfsAccess.kfs_isDirectory(basedir)) {
                System.out.println("KFS doesn't think " + basedir + " is a dir!");
                System.exit(1);

            }

            // Create a simple file with default replication (at most 3)
            String path = new String(basedir + "/foo.1");
            int fd;

            // fd is our file-handle to the file we are creating; this
            // file handle should be used in subsequent I/O calls on
            // the file.
            if ((fd = kfsAccess.kfs_create(path)) < 0) {
                System.out.println("Unable to call create");
                System.exit(1);
            }
            
            // Get the directory listings
            String [] entries;
            if ((entries = kfsAccess.kfs_readdir(basedir)) == null) {
                System.out.println("Readdir failed");
                System.exit(1);
            }

            System.out.println("Readdir returned: ");
            for (int i = 0; i < entries.length; i++) {
                System.out.println(entries[i]);
            }

            // write something to the file
            int numBytes = 2048;
            char [] dataBuf = new char[numBytes];
            
            generateData(dataBuf, numBytes);
            
            String s = new String(dataBuf);
            byte[] buf = s.getBytes();

            int res = kfsAccess.kfs_write(fd, buf, 0, buf.length);
            if (res != buf.length) {
                System.out.println("Was able to write only: " + res);
            }

            // flush out the changes
            kfsAccess.kfs_sync(fd);

            // Close the file-handle
            kfsAccess.kfs_close(fd);
            
            // Determine the file-size
            long sz = kfsAccess.kfs_filesize(path);

            if (sz != buf.length) {
                System.out.println("System thinks the file's size is: " + sz);
            }

            // rename the file
            String npath = new String(basedir + "/foo.2");
            kfsAccess.kfs_rename(path, npath);

            if (kfsAccess.kfs_exists(path)) {
                System.out.println(path + " still exists after rename!");
                System.exit(1);
            }

            int fd1 = kfsAccess.kfs_create(path);
            kfsAccess.kfs_close(fd1);

            if (!kfsAccess.kfs_exists(path)) {
                System.out.println(path + " doesn't exist");
                System.exit(1);
            }

            // try to rename and don't allow overwrite
            if (kfsAccess.kfs_rename(npath, path, false) == 0) {
                System.out.println("Rename with overwrite disabled succeeded!");
                System.exit(1);
            }

            // Remove the file
            kfsAccess.kfs_remove(path);

            // Verify that it is gone
            if (!kfsAccess.kfs_isFile(npath)) {
                System.out.println(npath + " is not a normal file!");
                System.exit(1);
            }

            // Re-open the file
            if ((fd = kfsAccess.kfs_open(npath, "rw")) < 0) {
                System.out.println("open on " + npath + "failed!");
                System.exit(1);
            }
            
            // read some bytes
            buf = new byte[128];
            res = kfsAccess.kfs_read(fd, buf, 0, 128);

            // Verify what we read matches what we wrote
            s = new String(buf);
            for (int i = 0; i < 128; i++) {
                if (dataBuf[i] != s.charAt(i)) {
                    System.out.println("Data mismatch at char: " + i);
                }
            }
            
            // seek to offset 40
            kfsAccess.kfs_seek(fd, 40);

            // Seek and verify that we are we think we are
            sz = kfsAccess.kfs_tell(fd);
            if (sz != 40) {
                System.out.println("After seek, we are at: " + sz);
            }

            kfsAccess.kfs_close(fd);

            // remove the file
            kfsAccess.kfs_remove(npath);

            // remove the dir
            if (kfsAccess.kfs_rmdir(basedir) < 0) {
                System.out.println("unable to remove: " + basedir);
                System.exit(1);
            }
            System.out.println("All done...Test passed!");

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to setup KfsAccess");
            System.exit(1);
        }
    }

    private static Random randGen = new Random(100);

    private static void generateData(char buf[], int numBytes)
    {
        int i;
            
        for (i = 0; i < numBytes; i++) {
            buf[i] = (char) ('a' + (randGen.nextInt(26)));
        }
    }

}
