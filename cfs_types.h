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

#include <sys/types.h>
#include <sys/stat.h>

// Maximum length (including NUL) of a key in the underlying key/value store.
// NB: a zero-length string means no key, e.g. for a zero-length file.
#define		CFS_MAX_KEY_LEN		64
#define CopyKey(d,s) strncpy((d),(s),CFS_MAX_KEY_LEN)

// Maximum length (including NUL) of a single path component.
#define		CFS_MAX_NAME_LEN	32
#define CopyName(d,s) strncpy((d),(s),CFS_MAX_NAME_LEN)

// superblock is stored as <prefix>_sb
// inodes are stored as <prefix>_i_NNN
// data blocks are stored as <prefix>_d_NNN
// NNN is up to CFS_INDEX_DIGITS long.
#define CFS_INDEX_DIGITS	9
#define CFS_MAX_PREFIX_LEN	(CFS_MAX_KEY_LEN - CFS_INDEX_DIGITS - 4)

typedef unsigned long	cfs_block_idx;
typedef unsigned long	cfs_offset_t;
typedef unsigned long	cfs_size_t;

// NB: data should *not* go in the inode, except for very short files as a
// special case.  Thus, when we do go to multiple blocks per file, we can
// only rewrite the keys corresponding to changed blocks instead of having
// to rewrite the one key for the whole file.

// TBD: to handle larger files we'll need indirect blocks etc.  Blech.
#define CFS_MAX_BLOCKS	2048
#define CFS_BLOCK_SIZE	8192
#define CFS_NO_BLOCK	~0

typedef struct {
	mode_t		type;	// Only S_IFMT bits matter
	unsigned long	size;
	// TBD: perms, times, etc. should go here.
	cfs_block_idx	data[CFS_MAX_BLOCKS];
} CfsInode;

typedef struct {
	char	name[CFS_MAX_NAME_LEN];
	char	inode_key[CFS_MAX_KEY_LEN];
	// TBD: we have to have inum anyway for readdir etc., so we don't need
	// to store inode_key as well (we could regenerate it whenever we want)
	int	inum;
	int	mode;
} CfsDirEntry;

typedef struct {
	char		prefix[CFS_MAX_NAME_LEN];
	char		root_dir_key[CFS_MAX_KEY_LEN];
	unsigned long	next_ialloc;
	unsigned long	next_dalloc;
} CfsSuperBlock;
