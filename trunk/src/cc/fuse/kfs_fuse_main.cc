//---------------------------------------------------------- -*- Mode: C++ -*-
// $Id: //depot/SOURCE/OPENSOURCE/kfs/src/cc/fuse/kfs_fuse_main.cc#2 $
//
// Created 2006/11/01
// Author: Blake Lewis (Kosmix Corp.)
//
// Copyright (C) 2006 Kosmix Corp.
//
// This file is part of Kosmos File System (KFS).
//
// KFS is free software: you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by
// the Free Software Foundation under version 3 of the License.
//
// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see
// <http://www.gnu.org/licenses/>.
// 
//----------------------------------------------------------------------------

#include "libkfsClient/KfsClient.h"

extern "C" {
#define FUSE_USE_VERSION	25
#define _FILE_OFFSET_BITS	64
#include <fuse.h>
#include <sys/stat.h>
}

static char *FUSE_KFS_PROPERTIES = "./kfs.prp";
static KfsClient *client;

void *
fuse_init()
{
	client = KFS::getKfsClient();
	client->Init(FUSE_KFS_PROPERTIES);
	return client->IsInitialized() ? client : NULL;
}

void
fuse_destroy(void *cookie)
{
	// KfsClient handle is a singleton.  So, don't "delete" it
	client = NULL;
}

static int
fuse_getattr(const char *path, struct stat *s)
{
	return client->Stat(path, *s);
}

static int
fuse_mkdir(const char *path, mode_t mode)
{
	return client->Mkdir(path);
}

static int
fuse_unlink(const char *path)
{
	return client->Remove(path);
}

static int
fuse_rmdir(const char *path)
{
	return client->Rmdir(path);
}

static int
fuse_rename(const char *src, const char *dst)
{
	return client->Rename(src, dst, false);
}

static int
fuse_truncate(const char *path, off_t size)
{
	int fd = client->Open(path, O_WRONLY);
	if (fd < 0)
		return fd;
	int status = client->Truncate(fd, size);
	client->Close(fd);
	return status;
}

static int
fuse_open(const char *path, struct fuse_file_info *finfo)
{
	return client->Open(path, finfo->flags);
}

static int
fuse_read(const char *path, char *buf, size_t nread, off_t off,
		struct fuse_file_info *finfo)
{
	int fd = client->Open(path, O_RDONLY);
	if (fd < 0)
		return fd;
	int status = client->Seek(fd, off, SEEK_SET);
	if (status == 0)
		status = client->Read(fd, buf, nread);
	client->Close(fd);
	return status;
}

static int
fuse_write(const char *path, const char *buf, size_t nwrite, off_t off,
		struct fuse_file_info *finfo)
{
	int fd = client->Open(path, O_WRONLY);
	if (fd < 0)
		return fd;
	int status = client->Seek(fd, off, SEEK_SET);
	if (status == 0)
		status = client->Write(fd, buf, nwrite);
	client->Close(fd);
	return status;
}

static int
fuse_flush(const char *path, struct fuse_file_info *finfo)
{
	int fd = client->Fileno(path);
	if (fd < 0)
		return fd;
	return client->Sync(fd);
}

static int
fuse_fsync(const char *path, int flags, struct fuse_file_info *finfo)
{
	int fd = client->Open(path, O_RDONLY);
	if (fd < 0)
		return fd;
	return client->Sync(fd);
}

static int
fuse_readdir(const char *path, void *buf,
		fuse_fill_dir_t filler, off_t offset,
		struct fuse_file_info *finfo)
{
	vector <KfsFileAttr> contents;
	int status = client->ReaddirPlus(path, contents);
	if (status < 0)
		return status;
	int n = contents.size();
	for (int i = 0; i != n; i++) {
		struct stat s;
		bzero(&s, sizeof s);
		s.st_ino = contents[i].fileId;
		s.st_mode = contents[i].isDirectory ? S_IFDIR : S_IFREG;
		if (filler(buf, contents[i].filename.c_str(), &s, 0) != 0)
			break;
	}
	return 0;
}

struct fuse_operations ops = {
	fuse_getattr,
	NULL,			/* readlink */
	NULL,			/* getdir */
	NULL,			/* mknod */
	fuse_mkdir,
	fuse_unlink,
	fuse_rmdir,
	NULL,			/* symlink */
	fuse_rename,
	NULL,			/* link */
	NULL,			/* chmod */
	NULL,			/* chown */
	fuse_truncate,
	NULL,			/* utime */
	fuse_open,
	fuse_read,
	fuse_write,
	NULL,			/* statfs */
	fuse_flush,		/* flush */
	NULL,			/* release */
	fuse_fsync,		/* fsync */
	NULL,			/* setxattr */
	NULL,			/* getxattr */
	NULL,			/* listxattr */
	NULL,			/* removexattr */
	NULL,			/* opendir */
	fuse_readdir,
	NULL,			/* releasedir */
	NULL,			/* fsyncdir */
	fuse_init,
	fuse_destroy,
	NULL,			/* access */
	NULL,			/* create */
	NULL,			/* ftruncate */
	NULL			/* fgetattr */
};

int
main(int argc, char **argv)
{
	return fuse_main(argc, argv, &ops);
}
