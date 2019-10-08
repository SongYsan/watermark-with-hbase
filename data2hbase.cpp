/*将data.txt的数据写入hbase数据库*/
/*Write data of "data.txt" to hbase database.*/
#include <iostream>
#include <stdio.h>
#include <string>
#include <vector>
#include <algorithm>
#include <fstream>
#include <string.h>
#include <sstream>
#include <stdlib.h>
#include "THBaseService.h"
#include <config.h>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hadoop::hbase::thrift2;

using boost::shared_ptr;

//写入一行数据
/*Write a row of data to the database*/
void writedb(int argc, char** argv, vector<string>& tempstr)
{
	fprintf(stderr, "writedb start\n");
	int port = atoi(argv[2]);
	boost::shared_ptr<TSocket> socket(new TSocket(argv[1], port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	try 
	{
		transport->open();
		printf("open\n");
		THBaseServiceClient client(protocol);
		std::vector<TPut> puts;
		const std::string table("hbase_test");

		TPut put;
		std::vector<TColumnValue> cvs;
		//put data
		put.__set_row(tempstr[0]);
		TColumnValue tcv;
		tcv.__set_family("info");
		//insert the first column
		tcv.__set_qualifier("age");
		tcv.__set_value(tempstr[1]);
		cvs.insert(cvs.end(), tcv);
		//insert the second column
		tcv.__set_qualifier("hours-per-week");
		tcv.__set_value(tempstr[2]);
		cvs.insert(cvs.end(), tcv);
		put.__set_columnValues(cvs);
		puts.insert(puts.end(), put);

		client.putMultiple(table, puts);
		puts.clear();

		transport->close();
		printf("close\n");
	} 
	catch (const TException &tx) 
	{
		std::cerr << "ERROR(exception): " << tx.what() << std::endl;
	}
	fprintf(stderr, "writedb stop\n");
}

/*读取数据库文件并逐行写入*/
/*Read "data.txt" and write them to database row by row.*/
bool mReadDbFile(int argc, char** argv, string dbFile)
{
	int li = 32560;
	char line[32560];                                    
	ifstream fin(dbFile.c_str());
	if(!fin.is_open())
	{
		cout << "open file failed！" << endl;
		return false;
	}
	else cout <<"open file sucessfully！" << endl;
	bool isFirstRaw = true; int h = 0;
	
	while (fin.getline(line, li))                       
	{
		if(isFirstRaw)  //it doesn't needed if we don't have the attribute row.                              
		{
			isFirstRaw = false;
			continue;
		}
		int len = strlen(line);                            
		int p = 0;                                         
		string temp; 
		vector<string>tempstr;                              
		while(p < len)
		{
			if(p == len - 2)                               
			{
				temp += line[p];
				tempstr.push_back(temp);
				break;
			}
			else if(line[p] == '\t')                             
			{
				tempstr.push_back(temp);
				temp = "";
			}
			else
			{
				temp += line[p];
			}
			p++;
		}
		temp.clear();
		
		writedb(argc, argv, tempstr); //Write a saved row of data to the database
	}
	fin.clear();
	fin.close();
	return true;
}

int main(int argc, char** argv)
{
	if(argc != 3) {
		fprintf(stderr, "param  is :XX ip port\n");
		return -1;
	}
	mReadDbFile(argc, argv, "/home/hadoop/adult.txt");
	return 0;
}