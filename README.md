# watermark_with_hbase
Using hbase as an example to apply database watermarking algorithm to big data

## Compiler Environment

Operating System:Centos7

Hadoop:2.6.0

boost:1.47.0

libevent:2.0.12-stable

Hbase:0.98.8

Thrift:0.7.0

jdk:1.8.0_221

## Note

We use hbase in the Standalone mode.

## How to run it

1.You should start related server.

2.Create a table which include two columns in the hbase.

3.Compile the data2hbase.cpp and run it to write data to database.

*g++ -DHAVE_NETINET_IN_H -o Data2HbaseClient -I/usr/local/thrift/include/thrift -I./gen-cpp -L/usr/local/thrift/lib data2hbase.cpp ./gen-cpp/hbase_types.cpp ./gen-cpp/hbase_constants.cpp ./gen-cpp/THBaseService.cpp -lthrift -g

*./Data2HbaseClient 0.0.0.0 9090

4.Compile the watermark.cpp and excute the watermarking algorithm.

*g++ -DHAVE_NETINET_IN_H -o WatermarkClient -I/usr/local/thrift/include/thrift -I./gen-cpp -L/usr/local/thrift/lib watermark.cpp SHA1.cpp ./gen-cpp/hbase_types.cpp ./gen-cpp/hbase_constants.cpp ./gen-cpp/THBaseService.cpp -lthrift -g

*./WatermarkClient 0.0.0.0 9090


