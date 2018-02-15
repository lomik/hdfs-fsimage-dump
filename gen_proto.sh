#!/bin/sh

cd `dirname $0`
ROOT=`pwd`
PACKAGE=github.com/lomik/hdfs-fsimage-dump

rm -rf ${ROOT}/pb
mkdir -p ${ROOT}/pb/{hadoop_common,hadoop_hdfs,fsimage}

protoc \
	--gogofast_out=${ROOT}/pb/hadoop_common/ \
	-I${ROOT}/hadoop_protocols/common \
	${ROOT}/hadoop_protocols/common/Security.proto

protoc \
	--gogofast_out=Mhdfs.proto=hadoop_hdfs,MSecurity.proto=$PACKAGE/pb/hadoop_common:${ROOT}/pb/hadoop_hdfs \
	-I${ROOT}/hadoop_protocols/common \
	-I${ROOT}/hadoop_protocols/hdfs \
	-I${ROOT}/hadoop_protocols ${ROOT}/hadoop_protocols/hdfs/*.proto

protoc \
	--gogofast_out=Mhdfs.proto=$PACKAGE/pb/hadoop_hdfs,Macl.proto=$PACKAGE/pb/hadoop_hdfs,Mxattr.proto=$PACKAGE/pb/hadoop_hdfs:${ROOT}/pb \
	-I${ROOT}/hadoop_protocols/common \
        -I${ROOT}/hadoop_protocols/hdfs \
        -I${ROOT}/hadoop_protocols/hdfs/fsimage \
	-I${ROOT}/hadoop_protocols ${ROOT}/hadoop_protocols/hdfs/fsimage/fsimage.proto

mv ${ROOT}/pb/fsimage ${ROOT}/pb/hadoop_hdfs_fsimage
