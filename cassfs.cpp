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

#define THRIFT_HOST "localhost"
#define THRIFT_PORT 9160

inline void
IndexToDataKey (cfs_block_idx index, char * pfx, char * key)
{
	snprintf(key,CFS_MAX_KEY_LEN,"%s_d_%0*d",
		pfx, CFS_INDEX_DIGITS, index);
}

inline void
IndexToInodeKey (cfs_block_idx index, char * pfx, char * key)
{
	snprintf(key,CFS_MAX_KEY_LEN,"%s_i_%0*d",
		pfx, CFS_INDEX_DIGITS, index);
}

CassFs::CassFs () :
	socket(new TSocket(THRIFT_HOST, THRIFT_PORT)),
	transport(new TBufferedTransport(socket)),
	protocol(new TBinaryProtocol(transport))
{
	mytable			= "Keyspace1";
	mycolumn.column_family	= "Standard1";
	mycolumn.column		= "data";
	mycolumn.__isset.column	= true;
	timestamp		= time(NULL);
	
	client		= new CassandraClient(protocol);
	transport->open();
	
	mounted = 0;
}

CassFs::~CassFs ()
{
	transport->close();
}

// TBD
// We should only really write the superblock once per N alloc-index updates,
// where N might be quite large.  If we increment by N when we mount, then we
// can keep track of indices privately.  We'd risk "leaking" some if we
// crashed before we used up our current allotment (which won't matter much
// given the size of the index space) but we'd easily avoid the problem of
// collision after a re-mount.
int
CassFs::WriteSuperBlock (void)
{
	string		sb_name;
	string		b64data;
	
	sb_name = sb.prefix;
	sb_name += "_sb";
	
	b64data = base64_encode(UCCP(&sb),sizeof(sb));
	cout << "writing " << sb_name << endl;
	client->insert(mytable,sb_name,mycolumn,b64data,timestamp++,ONE);
	
	return 0;
}

int
CassFs::MountFs (char * prefix)
{
	string			sb_name;
	string			sbdata;
	string			ridata;
	ColumnOrSuperColumn	waste;
	
	if (mounted && !strcmp(sb.prefix,prefix)) {
		cout << "already mounted " << prefix << endl;
		return 0;
	}
	mounted = 0;
	
	sb_name = prefix;
	sb_name += "_sb";
	
	try {
		client->get(waste,mytable,sb_name,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing superblock" << endl;
		return EIO;
	}
	
	sbdata = base64_decode(waste.column.value);
	if (sbdata.size() != sizeof(sb)) {
		cout << "got " << sbdata.size() << "/" << sizeof(sb)
		     << " for superblock" << endl;
		return EIO;
	}
	memcpy(&sb,sbdata.data(),sizeof(sb));
	cout << "prefix = " << sb.prefix << endl;
	cout << "root_dir_key = " << sb.root_dir_key << endl;
	cout << "next_ialloc = " << sb.next_ialloc << endl;
	cout << "next_dalloc = " << sb.next_dalloc << endl;
	
	try {
		client->get(waste,mytable,sb.root_dir_key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing root inode" << endl;
	}
	
	ridata = base64_decode(waste.column.value);
	if (ridata.size() != sizeof(root)) {
		cout << "got " << ridata.size() << "/" << sizeof(root)
		     << " for root inode" << endl;
		return EIO;
	}
	memcpy(&root,ridata.data(),sizeof(root));
	cout << "root data = " << root.data << endl;
	
	if (!S_ISDIR(root.type)) {
		cout << "root is not a dir?!?" << endl;
	}

	mounted = 1;
	return 0;
}

int
CassFs::LookupOne (CfsInode * parent, char * elem, CfsInode * child)
{
	CfsDirEntry *		r_data;
	string			rddata;
	ColumnOrSuperColumn	waste;
	int			i;
	string			idata;
	char			data_key[CFS_MAX_KEY_LEN];
	
	try {
		IndexToDataKey(parent->data[0],sb.prefix,data_key);
		client->get(waste,mytable,data_key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing directory data" << endl;
		return EIO;
	}
	
	rddata = base64_decode(waste.column.value);
	if (rddata.size() % sizeof(*r_data)) {
		cout << "got " << rddata.size() << "%" << sizeof(*r_data)
		     << " for dir " << data_key << endl;
		return EIO;
	}
	r_data = (CfsDirEntry *)rddata.data();
	
	for (i = 0; i < rddata.size(); i += sizeof(*r_data),++r_data) {
		if (!strcmp(r_data->name,elem)) {
			break;
		}
	}
	if (i >= rddata.size()) {
		cout << "elem " << elem << " not found" << endl;
		return ENOENT;
	}
	
	try {
		client->get(waste,mytable,r_data->inode_key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing inode " << r_data->inode_key << " " << endl;
	}
	
	idata = base64_decode(waste.column.value);
	if (idata.size() != sizeof(*child)) {
		cout << "got " << idata.size() << "/" << sizeof(*child)
		     << " for inode " << r_data->inode_key << endl;
		return EIO;
	}
	memcpy(child,idata.data(),sizeof(*child));
	
	return 0;
}

char *
mysplit (char * haystack, char * &work)
{
	if (work == (char *)(-1)) {
		return NULL;
	}
	if (work) {
		*work = '/';
		haystack = work + 1;
	}
	
	work = index(haystack,'/');
	if (work) {
		*work = '\0';
	}
	else {
		work = (char *)(-1);
	}
	return haystack;
}

// NB: we might trash *new_inode even if we fail half-way
int
CassFs::LookupAll (char * path, CfsInode ** an_inode_p)
{
	CfsInode *	new_inode	= *an_inode_p;
	CfsInode *	cur_inode;
	char *		elem;
	int		rc;
	char *		sep		= NULL;
	
	cur_inode = &root;
	for (elem = mysplit(path,sep); elem; elem = mysplit(path,sep)) {
		if (*elem == '\0') {
			continue;
		}
		if (!S_ISDIR(cur_inode->type)) {
			cout << "tried to traverse non-dir " << elem << endl;
			return 0;
		}
		cout << "descend into " << elem << endl;
		rc = LookupOne(cur_inode,elem,new_inode);
		if (rc != 0) {
			return rc;
		}
		cur_inode = new_inode;
	}
	
	*an_inode_p = cur_inode;
	return 0;
}

int
CassFs::CreateDir (char * parent_key, char * inode_key, cfs_block_idx data_idx)
{
	CfsInode	r_inode;
	CfsDirEntry	r_data[2];
	string		b64data;
	char		data_key[CFS_MAX_KEY_LEN];
	
	r_inode.type = S_IFDIR;
	r_inode.data[0] = data_idx;
	b64data = base64_encode(UCCP(&r_inode),sizeof(r_inode));
	cout << "writing " << inode_key << endl;
	client->insert(mytable,inode_key,mycolumn,b64data,timestamp++,ONE);
	
	CopyName(r_data[0].name,".");
	CopyKey(r_data[0].inode_key,inode_key);
	CopyName(r_data[1].name,"..");
	CopyKey(r_data[1].inode_key,parent_key);
	b64data = base64_encode(UCCP(&r_data),sizeof(r_data));
	IndexToDataKey(data_idx,sb.prefix,data_key);
	cout << "writing " << data_key << endl;
	client->insert(mytable,data_key,mycolumn,b64data,timestamp++,ONE);
	
	return 0;
}

void
CassFs::Put (char * key, char * value)
{	
	client->insert(mytable,key,mycolumn,value,timestamp++,ONE);
}

int
CassFs::Get (ColumnOrSuperColumn &waste, char * key)
{
	try {
		client->get(waste,mytable,key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "no such key" << endl;
		return EIO;
	}
	catch (TException &tx) {
		cout << "unknown exception " << tx.what() << endl;
		return EIO;
	}
	
	return 0;
}

void
CassFs::Del (char * key)
{
	client->remove(mytable,key,mycolumn,timestamp++,ONE);
}
	
int
CassFs::Mkfs (char * prefix)
{
	string		sb_name;
	string		b64data;
	int		rc;
		
	if (strlen(prefix) > CFS_MAX_PREFIX_LEN) {
		cerr << "prefix too long" << endl;
		return E2BIG;
	}
	
	CopyName(sb.prefix,prefix);
	IndexToInodeKey(1,prefix,sb.root_dir_key);
	sb.next_ialloc = 2;
	sb.next_dalloc = 1;
	
	rc = CreateDir(sb.root_dir_key,sb.root_dir_key,0);
	if (rc != 0) {
		return rc;
	}
	
	rc = WriteSuperBlock();
	if (rc != 0) {
		return rc;
	}
	
	return 0;
}

int
CassFs::Mkdir (char * path)
{
	CfsDirEntry *		r_data;
	string			rddata;
	ColumnOrSuperColumn	waste;
	int			i;
	int			rc;
	CfsInode		my_inode;
	CfsInode *		cur_inode	= &my_inode;
	char *			split;
	char			inode_key[CFS_MAX_KEY_LEN];
	char			pdata_key[CFS_MAX_KEY_LEN];
	CfsDirEntry *		new_dir;
	CfsDirEntry *		new_data;
	string			b64data;
	
	split = rindex(path,'/');
	if (!split || (split[1] == '\0')) {
		cout << "no new path component" << endl;
		return EINVAL;
	}
	*(split++) = '\0';
	
	if (!mounted) {
		return ENODEV;
	}

	rc = LookupAll(path,&cur_inode);
	if (rc != 0) {
		return rc;
	}
	
	try {
		IndexToDataKey(cur_inode->data[0],sb.prefix,pdata_key);
		client->get(waste,mytable,pdata_key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing dir contents for mkdir" << endl;
	}
	
	rddata = base64_decode(waste.column.value);
	if (rddata.size() % sizeof(*r_data)) {
		cout << "got " << rddata.size() << "%" << sizeof(*r_data)
		     << " for dir " << pdata_key << endl;
		return EIO;
	}
	r_data = (CfsDirEntry *)rddata.data();
	
	for (i = 0; i < (rddata.size() / sizeof(*r_data)); ++i) {
		if (!strcmp(r_data->name,split)) {
			cout << split << " already exists" << endl;
			return EEXIST;
		}
		++r_data;
	}
	
	cout << "checked " << i << " entries" << endl;
	
	r_data = (CfsDirEntry *)rddata.data();	// reset to get . entry
	IndexToInodeKey(sb.next_ialloc++,sb.prefix,inode_key);
	rc = CreateDir(r_data->inode_key,inode_key,sb.next_dalloc++);
	if (rc != 0) {
		return rc;
	}

	new_dir = (CfsDirEntry *)malloc(sizeof(*new_dir)*(i+1));
	if (!new_dir) {
		cout << "could not allocate expanded directory" << endl;
		return ENOMEM;
	}
	memcpy(new_dir,r_data,rddata.size());
	
	new_data = new_dir + i;
	CopyName(new_data->name,split);
	CopyKey(new_data->inode_key,inode_key);
	new_data->inum = sb.next_ialloc - 1;
	new_data->mode = S_IFDIR;
	b64data = base64_encode(UCCP(new_dir),sizeof(*new_dir)*(i+1));
	cout << "rewriting " << pdata_key << " with " << i+1
	     << " entries" << endl;
	client->insert(mytable,pdata_key,mycolumn,b64data,timestamp++,ONE);
	
	(void)WriteSuperBlock();	// ... to update the alloc indices
	return 0;
}

int
CassFs::List (char * path, cfs_list_cb_t * cb)
{
	CfsDirEntry *		r_data;
	string			rddata;
	ColumnOrSuperColumn	waste;
	int			i;
	int			rc;
	CfsInode		my_inode;
	CfsInode *		cur_inode	= &my_inode;
	char			data_key[CFS_MAX_KEY_LEN];
	
	if (!mounted) {
		return ENODEV;
	}
	
	rc = LookupAll(path,&cur_inode);
	if (rc != 0) {
		return rc;
	}
	
	if (!S_ISDIR(cur_inode->type)) {
		cout << path << " not a directory" << endl;
		return 0;
	}
	
	try {
		IndexToDataKey(cur_inode->data[0],sb.prefix,data_key);
		client->get(waste,mytable,data_key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing dir contents for list" << endl;
	}
	
	rddata = base64_decode(waste.column.value);
	if (rddata.size() % sizeof(*r_data)) {
		cout << "got " << rddata.size() << "%" << sizeof(*r_data)
		     << " for dir " << data_key << endl;
		return EIO;
	}
	r_data = (CfsDirEntry *)rddata.data();
	
	for (i = 0; i < rddata.size(); i += sizeof(*r_data),++r_data) {
		cb(r_data->name,r_data->inum,r_data->mode);
	}
	
	return 0;
}

int
CassFs::OpenFile (char * dir_key, char * fn, int create,
		  char * inode_key, CfsInode * inodep)
{
	ColumnOrSuperColumn	waste;
	string			rddata;
	CfsDirEntry *		r_data;
	int			i;
	string			b64data;
	CfsDirEntry *		new_dir;
	int			found		= 0;

	try {
		client->get(waste,mytable,dir_key,mycolumn,ONE);
	}
	catch (NotFoundException &tx) {
		cout << "missing dir contents for " << dir_key << endl;
	}
	
	rddata = base64_decode(waste.column.value);
	if (rddata.size() % sizeof(*r_data)) {
		cout << "got " << rddata.size() << "%" << sizeof(*r_data)
		     << " for dir " << dir_key << endl;
		return EIO;
	}
	r_data = (CfsDirEntry *)rddata.data();
	
	for (i = 0; i < (rddata.size() / sizeof(*r_data)); ++i) {
		if (!strcmp(r_data->name,fn)) {
			found = 1;
			break;
		}
		++r_data;
	}
	
	if (found) {
		CopyKey(inode_key,r_data->inode_key);
		cout << "fetching " << inode_key << endl;
		try {
			client->get(waste,mytable,inode_key,mycolumn,ONE);
		}
		catch (NotFoundException &tx) {
			cout << "missing inode " << inode_key << endl;
		}
		b64data = base64_decode(waste.column.value);
		if (b64data.size() != sizeof(*inodep)) {
			cout << "got " << b64data.size() << "/"
			     << sizeof(*inodep) << " for " << inode_key << endl;
			     return EIO;
		}
		memcpy(inodep,b64data.data(),sizeof(*inodep));
		if (!S_ISREG(inodep->type)) {
			cout << "writing to non-file type "
			     << hex << inodep->type << endl;
			return EISDIR;
		}
	}
	else {
		if (!create) {
			return ENOENT;
		}
		cout << "creating new " << fn << endl;
		IndexToInodeKey(sb.next_ialloc++,sb.prefix,inode_key);
			
		new_dir = (CfsDirEntry *)malloc(sizeof(*new_dir)*(i+1));
		if (!new_dir) {
			cout << "could not allocate expanded directory" << endl;
			return ENOMEM;
		}
		memcpy(new_dir,rddata.data(),rddata.size());
		
		CopyName(new_dir[i].name,fn);
		CopyKey(new_dir[i].inode_key,inode_key);
		new_dir[i].inum = sb.next_ialloc - 1;
		new_dir[i].mode = S_IFREG;
		b64data = base64_encode(UCCP(new_dir),sizeof(*new_dir)*(i+1));
		cout << "rewriting " << dir_key << " with " << i+1
		     << " entries" << endl;
		client->insert(mytable,dir_key,mycolumn,b64data,timestamp++,ONE);
		free(new_dir);
		
		inodep->type = S_IFREG;
		inodep->size - 0;
		for (i = 0; i < CFS_MAX_BLOCKS; ++i) {
			inodep->data[i] = CFS_NO_BLOCK;
		}
		(void)WriteSuperBlock();	// ... to update next_ialloc
	}
	
	return 0;
}

int
CassFs::Write (char * path, cfs_offset_t off, char * buf, cfs_size_t len)
{
	char *			split;
	CfsInode		my_inode;
	CfsInode *		cur_inode	= &my_inode;
	int			rc;
	char			dir_key[CFS_MAX_KEY_LEN];
	char			inode_key[CFS_MAX_KEY_LEN];
	char			data_key[CFS_MAX_KEY_LEN];
	string			b64data;
	string			odata;
	cfs_offset_t		ib_off;
	cfs_size_t		ib_len;
	cfs_block_idx		bnum;
	ColumnOrSuperColumn	waste;
	bool			allocated	= false;
	
	// TBD: Putting this much data on the stack makes my skin crawl.
	char			data[CFS_BLOCK_SIZE];
	char *			datap;
		
	if (!mounted) {
		return ENODEV;
	}

	split = rindex(path,'/');
	if (!split || (split[1] == '\0')) {
		cout << "no new path component" << endl;
		return EINVAL;
	}
	*split = '\0';
	
	rc = LookupAll(path,&cur_inode);
	*split = '/';
	if (rc != 0) {
		return rc;
	}
	
	IndexToDataKey(cur_inode->data[0],sb.prefix,dir_key);
	rc = OpenFile(dir_key,split+1,1,inode_key,&my_inode);
	if (rc != 0) {
		return rc;
	}
	if (my_inode.size < (off+len)) {
		cout << "increasing size to " << off+len << endl;
		my_inode.size = off + len;
	}
	
	while (len > 0) {
		// ib_ = Intra Block
		ib_off = off % CFS_BLOCK_SIZE;
		ib_len = CFS_BLOCK_SIZE - ib_off;
		if (ib_len > len) {
			ib_len = len;
		}
		bnum = off / CFS_BLOCK_SIZE;
		if (my_inode.data[bnum] == CFS_NO_BLOCK) {
			cout << "allocating block " << bnum << endl;
			my_inode.data[bnum] = sb.next_dalloc++;
			IndexToDataKey(my_inode.data[bnum],sb.prefix,data_key);
			memset(data,0,sizeof(data));
			datap = data;
			allocated = true;
		}
		else {
			cout << "modifying block " << bnum << endl;
			IndexToDataKey(my_inode.data[bnum],sb.prefix,data_key);
			try {
				client->get(waste,mytable,data_key,mycolumn,ONE);
			}
			catch (NotFoundException &tx) {
				cout << "missing data for " << data_key << endl;
				break;
			}
			odata = base64_decode(waste.column.value);
			if (odata.size() != CFS_BLOCK_SIZE) {
				cout << "bad size " << odata.size() << " for "
				     << data_key << endl;
				break;
			}
			datap = (char *)odata.data();
		}
		cout << "updating " << ib_off << ":" << ib_len << endl;
		memcpy(datap+ib_off,buf,ib_len);
		cout << "writing " << data_key << endl;
		b64data = base64_encode(UCCP(datap),CFS_BLOCK_SIZE);
		client->insert(mytable,data_key,mycolumn,b64data,timestamp++,ONE);
		off += ib_len;
		len -= ib_len;
		buf += ib_len;
	}
	
	// TBD: we really don't have to rewrite the inode if it's an old one,
	// we haven't changed the size, etc.  We'd have to redo the OpenFile
	// interface to know that, though.
	b64data = base64_encode(UCCP(&my_inode),sizeof(my_inode));
	cout << "writing " << inode_key << endl;
	client->insert(mytable,inode_key,mycolumn,b64data,timestamp++,ONE);
	
	if (allocated) {
		WriteSuperBlock();
	}
	
	return 0;
}

// TBD: use common routing for read and write since they're so similar

int
CassFs::Read (char * path, cfs_offset_t off, char * buf, cfs_size_t &in_len)
{
	char *			split;
	CfsInode		my_inode;
	CfsInode *		cur_inode	= &my_inode;
	int			rc;
	char			dir_key[CFS_MAX_KEY_LEN];
	char			inode_key[CFS_MAX_KEY_LEN];
	char			data_key[CFS_MAX_KEY_LEN];
	string			b64data;
	string			odata;
	cfs_offset_t		ib_off;
	cfs_size_t		ib_len;
	cfs_block_idx		bnum;
	ColumnOrSuperColumn	waste;
	cfs_size_t		len;
	
	// TBD: Putting this much data on the stack makes my skin crawl.
	char			data[CFS_BLOCK_SIZE];
	char *			datap;
		
	if (!mounted) {
		return ENODEV;
	}
	
	split = rindex(path,'/');
	if (!split || (split[1] == '\0')) {
		cout << "no new path component" << endl;
		return EINVAL;
	}
	*split = '\0';
	
	rc = LookupAll(path,&cur_inode);
	*split = '/';
	if (rc != 0) {
		return rc;
	}
	
	IndexToDataKey(cur_inode->data[0],sb.prefix,dir_key);
	rc = OpenFile(dir_key,split+1,0,inode_key,&my_inode);
	if (rc != 0) {
		return rc;
	}
	
	if (off >= my_inode.size) {
		cout << "read past EOF" << endl;
		in_len = 0;
		return 0;
	}
	
	if (in_len > (my_inode.size - off)) {
		in_len = my_inode.size - off;
		cout << "read crossed EOF - shortened to " << in_len << endl;
	}
	
	len = in_len;
	
	while (len > 0) {
		// ib_ = Intra Block
		ib_off = off % CFS_BLOCK_SIZE;
		ib_len = CFS_BLOCK_SIZE - ib_off;
		if (ib_len > len) {
			ib_len = len;
		}
		bnum = off / CFS_BLOCK_SIZE;
		if (my_inode.data[bnum] == CFS_NO_BLOCK) {
			cout << "empty block " << bnum << endl;
			memset(data,0,sizeof(data));
			datap = data;
		}
		else {
			cout << "reading block " << bnum << endl;
			IndexToDataKey(my_inode.data[bnum],sb.prefix,data_key);
			try {
				client->get(waste,mytable,data_key,mycolumn,ONE);
			}
			catch (NotFoundException &tx) {
				cout << "missing data for " << data_key << endl;
				break;
			}
			odata = base64_decode(waste.column.value);
			if (odata.size() != CFS_BLOCK_SIZE) {
				cout << "bad size " << odata.size() << " for "
				     << data_key << endl;
				break;
			}
			datap = (char *)odata.data();
		}
		cout << "updating " << ib_off << ":" << ib_len << endl;
		memcpy(buf,datap+ib_off,ib_len);
		off += ib_len;
		len -= ib_len;
		buf += ib_len;
	}
	
	return 0;
}
