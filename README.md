Using hbase as an example to apply database watermarking algorithm to big data

## Compiler Environment

* Operating System : Centos7

* Hadoop : 2.6.0

* boost : 1.47.0

* libevent : 2.0.12-stable

* Hbase : 0.98.8

* Thrift : 0.7.0

* jdk : 1.8.0_221

## Note

We use hbase in the Standalone mode.

## How to run it

1. You should start related server.
> start-dfs.sh

> start-yarn.sh

> /usr/local/hbase-0.98.8/bin/start-hbase.sh

> /usr/local/hbase-0.98.8/bin/hbase-daemon.sh start thrift2

> jps
```
4130 ResourceManager
3957 SecondaryNameNode
3639 NameNode
4922 HMaster
5309 Jps
5230 ThriftServer
4239 NodeManager
```


2. Create a table 'hbase_test' in the hbase.
> /usr/local/hbase-0.98.8/bin/hbase shell
> create 'hbase_test', 'info'
> scan 'hbase_test'
> exit


3. Compile the data2hbase.cpp and run it to write data to database.

> cd /home/hadoop

* `g++ -DHAVE_NETINET_IN_H -o Data2HbaseClient -I/usr/local/thrift/include/thrift -I./gen-cpp -L/usr/local/thrift/lib data2hbase.cpp ./gen-cpp/hbase_types.cpp ./gen-cpp/hbase_constants.cpp ./gen-cpp/THBaseService.cpp -lthrift -g`

* `./Data2HbaseClient 0.0.0.0 9090`

If there is any warning, try 

> source /etc/profile

4. Compile the watermark.cpp and excute the watermarking algorithm.

* `g++ -DHAVE_NETINET_IN_H -o WatermarkClient -I/usr/local/thrift/include/thrift -I./gen-cpp -L/usr/local/thrift/lib watermark.cpp SHA1.cpp ./gen-cpp/hbase_types.cpp ./gen-cpp/hbase_constants.cpp ./gen-cpp/THBaseService.cpp -lthrift -g`

* `./WatermarkClient 0.0.0.0 9090`


