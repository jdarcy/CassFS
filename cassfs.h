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

typedef void cfs_list_cb_t (char * name, int inum, int mode);

class CassFs {
private:
	shared_ptr<TTransport>	socket;
	shared_ptr<TTransport>	transport;
	shared_ptr<TProtocol>	protocol;
	CassandraClient *	client;
	string			mytable;
	ColumnPath		mycolumn;
	int			timestamp;
	CfsSuperBlock		sb;
	CfsInode		root;
	int			mounted;
	
public:
		CassFs		();
		~CassFs		();
	int	WriteSuperBlock	(void);
	int	MountFs		(char * prefix);
	int	LookupOne	(CfsInode * parent, char * elem,
				 CfsInode * child);
	int	LookupAll	(char * path, CfsInode ** an_inode_p);
	int	CreateDir	(char * parent_key, char * inode_key,
				 cfs_block_idx data_idx);
	int	OpenFile	(char * dir_key, char * fn, int create,
				 char * inode_key, CfsInode * inodep);
				 
	void	Put		(char * key, char * value);
	int	Get		(ColumnOrSuperColumn &waste, char * key);
	void	Del		(char * key);
	
	int	Mkfs		(char * prefix);
	int	Mount		(char * prefix);
	int	Mkdir		(char * path);
	int	List		(char * path, cfs_list_cb_t * cb);
	int	Write		(char * path, cfs_offset_t off,
				 char * buf, cfs_size_t len);
	int	Read		(char * path, cfs_offset_t off,
				 char * buf, cfs_size_t &in_len);
};

