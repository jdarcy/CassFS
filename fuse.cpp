/*
    This file is part of CassFS.
    Copyright 2010 Jeff Darcy <jeff@pl.atyp.us>

    CassFS is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    CassFS is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with CassFS.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <errno.h>
#include <stdio.h>
#include <iostream>

#include <boost/shared_ptr.hpp>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include "Cassandra.h"
#include "base64.h"
#include "cfs_types.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;

#include "cassfs.h"

extern "C" {

/*
 * Based in part on fusexmp.c, part of FUSE
 * Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
 * See http://fuse.sourceforge.net/ for original code and license.
 */

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
//#define _XOPEN_SOURCE 500
#endif

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif

struct my_opts {
	char *	host;
	char *	port;
	char *	name;
};

struct my_opts opts = { (char *)"localhost", (char *)"7777" };

struct fuse_opt my_opt_descs[] = {
	{ "host=%s", offsetof(struct my_opts,host) },
	{ "port=%s", offsetof(struct my_opts,port) },
	{ "name=%s", offsetof(struct my_opts,name) },
	{ NULL }
};

static int cfs_getattr(const char *path, struct stat *stbuf)
{
	CassFs *	cfs;
	int		rc;
	CfsInode	tmp;
	CfsInode *	my_inode	= &tmp;
	
	printf("in %s(%s)\n",__func__,path);
	
	cfs = (CassFs *)fuse_get_context()->private_data;
	rc = cfs->LookupAll((char *)path,&my_inode);
	if (rc) {
		return -rc;
	}
	cout << path << " => type " << hex << my_inode->type << ", size "
	     << dec << my_inode->size << endl;
	     
	stbuf->st_mode = my_inode->type | 0644;
	stbuf->st_size = my_inode->size;
	return 0;
}

static int cfs_access(const char *path, int mask)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_readlink(const char *path, char *buf, size_t size)
{
	printf("in %s(%s,%p,%u)\n",__func__,path,buf,size);
	return 0;
}

// TBD: Blech.  Pass this as an arg through CassFs::List and back.
void *		rd_buf;
fuse_fill_dir_t	rd_fnc;

void
cfs_list_cb (char * name, int inum, int mode)
{
	struct stat	st;
	
	if (rd_fnc) {
		cout << "returning " << name << endl;
		st.st_ino = inum;
		st.st_mode = mode >> 12;
		if (rd_fnc(rd_buf,name,&st,0)) {
			rd_fnc = NULL;
		}
	}
	else {
		cout << "skipping " << name << endl;
	}
}		

static int cfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	CassFs *	cfs;

	(void) offset;
	(void) fi;

	printf("in %s(%s,%p,%d)\n",__func__,path,buf,offset);
	cfs = (CassFs *)fuse_get_context()->private_data;
	rd_buf = buf;
	rd_fnc = filler;
	cfs->List((char *)path,cfs_list_cb);
	return 0;
}

static int cfs_mknod(const char *path, mode_t mode, dev_t rdev)
{
	CassFs *	cfs;
	int		rc;
	
	printf("in %s(%s)\n",__func__,path);
	
	if (!S_ISREG(mode)) {
		return -EOPNOTSUPP;
	}
	
	cfs = (CassFs *)fuse_get_context()->private_data;
	// TBD: fix the interface to CassFs::OpenFile, because this way is
	// cheesy as hell.
	rc = cfs->Write((char *)path,0,NULL,0);
	cout << __func__ << " got " << rc << " back from Write(0) for "
	     << path << endl;
	return -rc;
}

static int cfs_mkdir(const char *path, mode_t mode)
{
	CassFs *	cfs;
	int		rc;
	
	printf("in %s\n",__func__);
	
	cfs = (CassFs *)fuse_get_context()->private_data;
	rc = cfs->Mkdir((char *)path);
	return rc ? -rc : 0;
}

static int cfs_unlink(const char *path)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_rmdir(const char *path)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_symlink(const char *from, const char *to)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_rename(const char *from, const char *to)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_link(const char *from, const char *to)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_chmod(const char *path, mode_t mode)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_chown(const char *path, uid_t uid, gid_t gid)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_truncate(const char *path, off_t size)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_utimens(const char *path, const struct timespec ts[2])
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_open(const char *path, struct fuse_file_info *fi)
{
	printf("in %s\n",__func__);
	return 0;
}

static int cfs_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	CassFs *	cfs;
	int		rc;
	cfs_size_t	len;
	
	printf("in %s(%s,%d,%u)\n",__func__,path,offset,size);
	
	cfs = (CassFs *)fuse_get_context()->private_data;
	len = size;
	rc = cfs->Read((char *)path,(cfs_offset_t)offset,buf,len);
	return rc ? -rc : len;
}

static int cfs_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	CassFs *	cfs;
	int		rc;
	
	printf("in %s\n",__func__);
	
	cfs = (CassFs *)fuse_get_context()->private_data;
	rc = cfs->Write((char *)path,(cfs_offset_t)offset,(char *)buf,size);
	cout << __func__ << "got " << rc << " back from Write" << endl;
	return rc ? -rc : size;
}

static int cfs_statfs(const char *path, struct statvfs *stbuf)
{
	int res;

	printf("in %s\n",__func__);
	res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

void *
cfs_init (struct fuse_conn_info * not_used)
{
	CassFs *	cfs;
	
	(void)not_used;
	
	printf("in %s\n",__func__);
	cfs = new CassFs();
	cfs->MountFs(opts.name);
	return cfs;
}

// Apparently g++ doesn't like ".getattr = cfs_getattr" style initializers
// even within an "extern C" block.  Would it be too much to ask that they
// support *their own language extensions* consistently?
static struct fuse_operations cfs_oper = {
	cfs_getattr,
	cfs_readlink,
	NULL, /* getdir */
	cfs_mknod,
	cfs_mkdir,
	cfs_unlink,
	cfs_rmdir,
	cfs_symlink,
	cfs_rename,
	cfs_link,
	cfs_chmod,
	cfs_chown,
	cfs_truncate,
	NULL, /* utime */
	cfs_open,
	cfs_read,
	cfs_write,
	cfs_statfs,
	NULL, /* flush */
	NULL, /* release */
	NULL, /* fsync */
#ifdef HAVE_SETXATTR
	NULL,
	NULL,
	NULL,
	NULL,
#endif
	NULL, /* opendir */
	cfs_readdir,
	NULL, /* releasedir */
	NULL, /* fsyncdir */
	cfs_init,
	NULL, /* destroy */
	cfs_access,
	NULL, /* create */
	NULL, /* ftruncate */
	NULL, /* fgetattr */
	NULL, /* lock */
	cfs_utimens,
};


int
myfs_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
     return 1;
}

int main(int argc, char *argv[])
{
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

	umask(0);
	fuse_opt_parse(&args, &opts, my_opt_descs, myfs_opt_proc);
	printf("using %s:%s\n",opts.host,opts.port);
	return fuse_main(args.argc, args.argv, &cfs_oper, NULL);
}

} // extern "C"
