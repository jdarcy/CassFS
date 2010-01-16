THRIFT		= /opt/thrift
THRIFT_INC	= $(THRIFT)/include/thrift
THRIFT_LIB	= $(THRIFT)/lib
CASSANDRA	= thrift_gen
INCLUDES	= -I$(THRIFT_INC) -I$(CASSANDRA)
DEFINES		= -D_FILE_OFFSET_BITS=64 -DHAVE_SETXATTR
WARNINGS	= -Wunused-variable
CPPFLAGS	= $(INCLUDES) $(DEFINES) $(WARNINGS) -g
LDFLAGS		= -L$(THRIFT_LIB) -lthrift

THRIFT_OBJS	= cassandra_constants.o \
		  cassandra_types.o \
		  Cassandra.o
LIB_NAME	= cassfs
LIB_TARGET	= lib$(LIB_NAME).so
LIB_ONLY_OBJS	= cassfs.o base64.o
LIB_OBJS	= $(LIB_ONLY_OBJS) $(THRIFT_OBJS)

CLI_TARGET	= cassfs_cli
CLI_OBJS	= cli.o

FUSE_TARGET	= cassfs
FUSE_OBJS	= fuse.o

ALL		= $(LIB_TARGET) $(CLI_TARGET) $(FUSE_TARGET)
ALL_OBJS	= $(LIB_OBJS) $(CLI_OBJS) $(FUSE_OBJS)

all: $(ALL)

$(LIB_TARGET): $(LIB_OBJS)
	$(CXX) -shared -fPIC $(LIB_OBJS) -o $@

$(CLI_TARGET): $(CLI_OBJS) $(LIB_TARGET)
	$(CXX) $(CLI_OBJS) -L. -l$(LIB_NAME) $(LDFLAGS) -o $@

$(FUSE_TARGET): $(FUSE_OBJS) $(LIB_TARGET)
	$(CXX) $(FUSE_OBJS) -L. -l$(LIB_NAME) $(LDFLAGS) -lfuse -o $@

cassandra_constants.cpp: $(CASSANDRA)/cassandra_constants.cpp
	ln -s $(CASSANDRA)/$@ $@

cassandra_types.cpp: $(CASSANDRA)/cassandra_types.cpp
	ln -s $(CASSANDRA)/$@ $@

Cassandra.cpp: $(CASSANDRA)/Cassandra.cpp
	ln -s $(CASSANDRA)/$@ $@

clean:
	rm -f $(ALL_OBJS)

clobber mrproper realclean spotless: clean
	rm -f $(ALL)
