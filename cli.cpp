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


int
ExitWithUsage (char * prog)
{
	cerr << "Usage: " << prog << "cmd [cmd_args...]" << endl;
	cerr << "  put key value" << endl;
	cerr << "  get key" << endl;
	cerr << "  del key" << endl;
	cerr << "  mkfs fs_name" << endl;
	cerr << "  mount fs_name" << endl;
	cerr << "  mkdir path" << endl;
	cerr << "  list path" << endl;
	cerr << "  write path data [offset]" << endl;
	cerr << "  read path len [offset]" << endl;
	cerr << "--- NOT IMPLEMENTED YET ---" << endl;
	cerr << "  rmdir path" << endl;
	cerr << "  unlink path" << endl;
	cerr << "  stat path" << endl;
	return EINVAL;
}

int
PutCommand (int argc, char ** argv, CassFs * cfs)
{
	if (argc != 4) {
		return ExitWithUsage(argv[0]);
	}
	
	cfs->Put(argv[2],argv[3]);
	return 0;
}

int
GetCommand (int argc, char ** argv, CassFs * cfs)
{
	ColumnOrSuperColumn waste;
	
	if (argc != 3) {
		return ExitWithUsage(argv[0]);
	}
	
	if (cfs->Get(waste,argv[2]) == 0) {
		cout << waste.column.value << endl;
		cout << waste.column.timestamp << endl;
	}
	
	return 0;
}

// NB: deletion seems *permanent* even past a subsequent insert.
// Yes, that's either a bug or a really bad design decision.
int
DelCommand (int argc, char ** argv, CassFs * cfs)
{
	if (argc != 3) {
		return ExitWithUsage(argv[0]);
	}
	
	cfs->Del(argv[2]);
	return 0;
}

int
MkfsCommand (int argc, char ** argv, CassFs * cfs)
{
	if (argc != 3) {
		return ExitWithUsage(argv[0]);
	}
	
	return cfs->Mkfs(argv[2]);
}

int
MountCommand (int argc, char ** argv, CassFs * cfs)
{
	if (argc != 3) {
		return ExitWithUsage(argv[0]);
	}
	
	return cfs->MountFs(argv[2]);
}

int
MkdirCommand (int argc, char ** argv, CassFs * cfs)
{	
	if (argc != 3) {
		return ExitWithUsage(argv[0]);
	}
	
	return cfs->Mkdir(argv[2]);
}

int
RmdirCommand (int argc, char ** argv, CassFs * cfs)
{
	printf("in %s\n",__func__);
	return 0;
}

void
cli_list_cb (char * name, int inum, int mode)
{
	cout << name << " => " << inum << " (" << mode << ")" << endl;
}

int
ListCommand (int argc, char ** argv, CassFs * cfs)
{
	if (argc != 3) {
		return ExitWithUsage(argv[0]);
	}
	
	return cfs->List(argv[2],cli_list_cb);
}

int
WriteCommand (int argc, char ** argv, CassFs * cfs)
{
	cfs_offset_t	offset;
	
	switch (argc) {
	case 4:
		offset = 0;
		break;
	case 5:
		offset = strtol(argv[4],NULL,10);
		break;
	default:
		return ExitWithUsage(argv[0]);
	}
	
	return cfs->Write(argv[2],offset,argv[3],strlen(argv[3]));
}

int
ReadCommand (int argc, char ** argv, CassFs * cfs)
{
	cfs_offset_t	offset;
	cfs_size_t	size;
	cfs_size_t	i;
	cfs_size_t	done;
	char *		buf;
	int		rc;
	
	switch (argc) {
	case 4:
		offset = 0;
		break;
	case 5:
		offset = strtol(argv[4],NULL,10);
		break;
	default:
		return ExitWithUsage(argv[0]);
	}
	size = strtol(argv[3],NULL,10);

	buf = (char *)malloc(size);
	if (!buf) {
		cout << "could not allocate read buffer" << endl;
		return ENOMEM;
	}
	
	rc = cfs->Read(argv[2],offset,buf,size);
	if (rc != 0) {
		cout << "read failed (" << rc << ")" << endl;
		free(buf);
		return rc;
	}
	
	for (done = 0; done < size; done = i) {
		for (i = done; i < size; ++i) {
			if (buf[i] != '\0') {
				break;
			}
		}
		if (i > done) {
			cout << "blank: " << i-done << " bytes" << endl;
			continue;
		}
		if ((buf[i] < 0x20) || (buf[i] >= 0x7f)) {
			cout << "oddball: " << (int)buf[i++] << endl;
			continue;
		}
		cout << "string: ";
		for (;;) {
			cout << buf[i++];
			if (i >= size) {
				break;
			}
			if ((buf[i] < 0x20) || (buf[i] >= 0x7f)) {
				break;
			}
		}
		cout << endl;
	}
	
	free(buf);
	return 0;
}

int
UnlinkCommand (int argc, char ** argv, CassFs * cfs)
{
	printf("in %s\n",__func__);
	return 0;
}

int
StatCommand (int argc, char ** argv, CassFs * cfs)
{
	printf("in %s\n",__func__);
	return 0;
}

// Anonymous types are valid, dammit.
struct _gcc_sucks {
	const char *	cmd_name;
	int	(*cmd_func) (int, char **, CassFs *);
} cmd_table[] = {
	{ "put",	PutCommand	},
	{ "get",	GetCommand	},
	{ "del",	DelCommand	},
	{ "mkfs",	MkfsCommand	},
	{ "mount",	MountCommand	},
	{ "mkdir",	MkdirCommand	},
	{ "rmdir",	RmdirCommand	},
	{ "list",	ListCommand	},
	{ "write",	WriteCommand	},
	{ "read",	ReadCommand	},
	{ "unlink",	UnlinkCommand	},
	{ "stat",	StatCommand	},
	{ "quit",	NULL		},
	{ NULL }
};

int
main(int argc, char **argv)
{
	int		i;
	int 		rc;
	CassFs *	cfs;
	int		ac;
	char *		av[10];
	char		buf[100];
	char *		tok;
	
	cfs = new CassFs();
	
	while (fgets(buf,sizeof(buf),stdin)) {
		ac = 0;
		av[ac++] = argv[0];
		for (tok = strtok(buf," \n"); tok; tok = strtok(NULL," \n")) {
			av[ac++] = tok;
		}
		for (i = 0; cmd_table[i].cmd_name; ++i) {
			if (!strcmp(av[1],cmd_table[i].cmd_name)) {
				break;
			}
		}
		if (!cmd_table[i].cmd_name) {
			(void)ExitWithUsage(argv[0]);
			continue;
		}
		if (!cmd_table[i].cmd_func) {
			break;
		}
		rc = cmd_table[i].cmd_func(ac,av,cfs);
		if (rc == 0) {
			cout << "OK" << endl;
		}
		else {
			cout << "status = " << rc << endl;
		}
	}
	
	delete cfs;
	return rc;
}
