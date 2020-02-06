#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <math.h>
#include <fstream>
#include <algorithm>
#include "THBaseService.h"
#include <config.h>
#include <vector>
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include "SHA1.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::hadoop::hbase::thrift2;
using boost::shared_ptr;

const double EPS = 1.0e-8;

int mark[32561][2]; //The sigh of the watermark.Set it to 1 when watermark is embedded.
string HashString(string str);
string Watermark = "suijis"; //The watermark that will be embedded.
string BinaryWatermark; //Binary representation of watermark
string Key; //random key
int mNG=0; //number of groups
int l; //length of watermark
string mem; //Extracted watermarking information
int low[2];
int high[2];
int varit[2];

int sign(double x)
{
	return x < -EPS ? -1 : x > EPS;
}

string HashString(string str)
{
	CSHA1 sha1;
	string strReport;

#ifdef _UNICODE
	const size_t uAnsiLen = wcstombs(NULL, str.c_str(), 0) + 1;
	char* pszAnsi = new char[uAnsiLen + 1];
	wcstombs(pszAnsi, str.c_str(), uAnsiLen);

	sha1.Update((UINT_8*)& pszAnsi[0], strlen(&pszAnsi[0]));
	sha1.Final();
	sha1.ReportHashStl(strReport, CSHA1::REPORT_HEX_SHORT);
	cout << _T("Hash of the ANSI representation of the string:") << endl;
	cout << strReport << endl << endl;

	delete[] pszAnsi;
	sha1.Reset();

	sha1.Update((UINT_8*)str.c_str(), str.size() * sizeof(TCHAR));
	sha1.Final();
	sha1.ReportHashStl(strReport, CSHA1::REPORT_HEX_SHORT);
	cout << _T("Hash of the Unicode representation of the string:") << endl;
	cout << strReport << endl;
#else
	sha1.Update((UINT_8*)str.c_str(), str.size() * sizeof(TCHAR));
	sha1.Final();
	sha1.ReportHashStl(strReport, CSHA1::REPORT_HEX_SHORT);
	return strReport;
#endif
}

/*string to integer*/
int str2int(string str)
{
	int ret = 0;
	for (int i = 0; i < str.size(); i++)
		ret = ret * 10 + (int)(str[i] - '0');
	return ret;
}

/*integer to string*/
string int2str(int x)
{
	string ret;
	if (x == 0)
		return string("0");
	while (x)
	{
		ret += (char)(x % 10 + '0');
		x = x / 10;
	}
	reverse(ret.begin(), ret.end());
	return ret;
}

/*string to float*/
float str2float(string str)
{
	float ret = 0;
	/*for (int i = 0; i < str.size(); i++)
		ret = ret * 10 + (int)(str[i] - '0');*/
	const char* a;
	a = str.c_str();
	ret = atof(a);
	return ret;
}

/*float to integer*/
int float2int(float f)
{
	string a1;
	char buf[22];
	gcvt(f, 3, buf);
	a1 = buf;
	int i = 0;
	while (a1[i] != '.')i++;
	while (a1[i + 1] == 0)i++; i++;
	int ret = 0;
	for (i; i < a1.size(); i++)
		ret = ret * 10 + (int)(a1[i] - '0');
	return ret;
}

/*float to string*/
string float2str(float x)
{
	string ret;
	if (x == 0)
		return string("0");
	char buf[22];
	gcvt(x, 5, buf); 
	ret = buf;
	//while (x)
	//{
	//	ret += (char)(x % 10 + '0');
	//	x = x / 10;
	//}
	return ret;
}

/*Read a row of data from the database.Here are two integer data, and the function returns an vector of string types*/
vector<string> readdb(int argc, char** argv,string row) {
	vector<string> result;
	result.clear();
	//fprintf(stderr, "readdb start\n");
	int port = atoi(argv[2]);
	boost::shared_ptr<TSocket> socket(new TSocket(argv[1], port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	try {
		transport->open();
		//printf("open\n");
		THBaseServiceClient client(protocol);
		TResult tresult;
		TGet get;
		std::vector<TColumnValue> cvs;
		const std::string table("hbase_test");
		const std::string thisrow = row;
		//get data
		get.__set_row(thisrow);
		bool be = client.exists(table, get);
		//printf("exists result value = %d\n", be);
		client.get(tresult, table, get);
		vector<TColumnValue> list = tresult.columnValues;
		std::vector<TColumnValue>::const_iterator iter;
		for (iter = list.begin(); iter != list.end(); iter++) {
			//printf("%s, %s, %s\n",(*iter).family.c_str(),(*iter).qualifier.c_str(),(*iter).value.c_str());
			result.push_back((*iter).value.c_str());
		}
		transport->close();
		//printf("close\n");
	}
	catch (const TException& tx) {
		std::cerr << "ERROR(exception): " << tx.what() << std::endl;
	}
	//fprintf(stderr, "readdb stop\n");
	return result;
}

/*Write a row of data to the database*/
int writedb(int argc, char** argv, string row, vector<string>& result) {
	//fprintf(stderr, "writedb start\n");
	int port = atoi(argv[2]);
	boost::shared_ptr<TSocket> socket(new TSocket(argv[1], port));
	boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
	boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	try {
		transport->open();
		//printf("open\n");
		THBaseServiceClient client(protocol);
		std::vector<TPut> puts;
		const std::string table("hbase_test");

		TPut put;
		std::vector<TColumnValue> cvs;
		//put data
		put.__set_row(row);
		TColumnValue tcv;

		//insert the first row of data
		tcv.__set_family("info");
		tcv.__set_qualifier("age");
		tcv.__set_value(result[0]);
		cvs.insert(cvs.end(), tcv);

		//insert the second row of data
		tcv.__set_qualifier("hours-per-week");
		tcv.__set_value(result[1]);
		cvs.insert(cvs.end(), tcv);

		put.__set_columnValues(cvs);
		puts.insert(puts.end(), put);

		client.putMultiple(table, puts);
		puts.clear();
		transport->close();
		//printf("close\n");
	}
	catch (const TException& tx) {
		std::cerr << "ERROR(exception): " << tx.what() << std::endl;
	}
	//fprintf(stderr, "writedb stop\n");
	return 0;
}

/*Calculate the maximum and minimum values for each column,thus we can calculate the sensitivity of each column.*/
void MiniMax(int argc, char** argv)
{
	for (int i = 0; i < 32561; i++)
	{
		vector<string> result;
		result = readdb(argc, argv, int2str(i + 1));
		for (int j = 0; j < 2; j++)
		{
			int compare = str2int(result[j]);
			if (low[j] > compare)
			{
				low[j] = compare;
			}
			if (high[j] < compare)
			{
				high[j] = compare;
			}
		}
	}
	varit[0] = high[0] - low[0];
	varit[1] = high[1] - low[1];
}

/*This is the grouping function,and the function returns an integer value to indicate which group a row tuple belongs to*/
int DivideGroup(string row)
{
	/* Nu = H( Ks | H( Ks | Tu.pkey ) ) */
	string tempStr = HashString(Key + HashString(Key + row));
	//string tempStr = HashString(this->mKey + this->mTuple[i].mPrimeKey);
	long long number = 0; //
	for (int j = 0; j < tempStr.size(); j++)
	{
		long long tempLL = 0;
		if (tempStr[j] >= '0' && tempStr[j] <= '9')
			tempLL = tempStr[j] - '0';
		else tempLL = tempStr[j] - 'A' + 10;
		number = (number * 16 % mNG + tempLL) % mNG;
	}
	return number;
}


double noisyCount(int sensitivity, double epsilon)
{
	int d = rand();// % (RAND_MAX + 1);

	if (d == 0)
		d++;
	else if (d == RAND_MAX)
		d--;

	double uniform = (double)d / RAND_MAX - 0.5;
	double s = (double)sensitivity;
	return s / epsilon * sign(uniform) * log(1 - 2.0 * fabs(uniform));
}

/*Embed watermarks separately for the elements of each row*/
bool EmbedWatermark(int argc, char** argv, string row)
{
	double epsilon = 1;
	int GroupOfThisTuple = DivideGroup(row);
	int bit = BinaryWatermark[GroupOfThisTuple] - '0';
	vector<string> result;
	result = readdb(argc, argv, row);
	double tp, tp2;
	for (int i = 0; i < 2; i++)
	{
		int x = str2int(result[i]);
		tp = x + noisyCount(varit[i], epsilon);
		if (tp < low[i])
			tp = low[i];
		else if (tp > high[i])
			tp = high[i];
		else if (tp - int(tp) > 0.5)
			tp = tp + 1;
		int tpp = tp;
		int yz = x;
		if (tpp - yz > 8) //Beyond the range of distortion, we need to embed watermark.
		{
			mark[str2int(row) - 1][i] = 1;
			if (bit == 1)
			{
				if ((tpp - 8) % 2 == 0)
				{
					x = tpp - 7;
				}
				else x = tpp - 8;
			}
			else if (bit == 0)
			{
				if ((tpp - 8) % 2 == 0)
				{
					x = tpp - 8;
				}
				else x = tpp - 7;
			}//cout << yz << "," << this->mTuple[this->mTupleOfGroup[idx][jj].Th].mInt[0] << ">>";
			//if (mid == 8)cout << this->mTuple[this->mTupleOfGroup[idx][jj].Th].mInt[j]<<endl;
		}
		else if (yz - tpp > 8)
		{
			//cout << "the second situation" << endl;
			mark[str2int(row) - 1][i] = 1; //record position
			if (bit == 1)
			{
				if ((tpp + 8) % 2 == 1)
				{
					x = tpp + 8;
				}
				else
				{
					x = tpp + 7;
				}
			}
			else if (bit == 0)
			{
				if ((tpp + 8) % 2 == 0)
				{
					x = tpp + 8;
				}
				else
				{
					x = tpp + 7;
				}
			}
		}
		else
		{
			x = tpp;
		}
		result[i]=int2str(x);
	}
	//Write the result after embedding watermark.
	writedb(argc, argv, row, result);
}

/*Extracting the watermark,and the function returns the binary representation of the watermark*/
string ExtractWatermark(int argc, char** argv)
{
	char b;
	vector<string> result;
	//int b0[mNG] = { 0 }, b1[mNG] = { 0 };
	int b0[mNG], b1[mNG];
	memset(b0, 0, sizeof(b0));
	memset(b1, 0, sizeof(b1));
	for (int i = 0; i < 32561; i++)
	{
		int g = DivideGroup(int2str(i+1));
		result = readdb(argc, argv, int2str(i + 1));
		for (int j = 0; j < 2; j++)
		{
			int num = str2int(result[j]);
			if (mark[i][j] == 1)
			{
				if (num % 2 == 0)
				{
					b = '0';
					b0[g]++;
				}
				else
				{
					b = '1';
					b1[g]++;
				}
			}
		}
	}

	for (int k = 0; k < mNG; k++)
	{
		b = (b0[k] >= b1[k]) ? '0' : '1';
		mem = mem + b;
	}
	return mem; //the function returns the extracted binary watermark information
}

/*Calculate the bit error rate*/
double ExtractBer()
{
	int wm = 0; double ber = 0.0;
	for (int w = 0; w < mNG; w++)
	{
		if (BinaryWatermark[w] != mem[w])
			wm++;
	}
	ber = double(wm) / mNG * 100;
	return ber;
}

int main(int argc, char** argv)
{
	cout<<"initialing......"<<endl;
	//Variable initialization
	for (int i = 0; i < 32561; i++)
		for (int j = 0; j < 2; j++)
			mark[i][j] = 0;
	mNG = Watermark.size() * 8;
	l = 0;
	low[0] = 39;
	low[1] = 40;
	high[0] = 39;
	high[1] = 40;

	//Generating 16-bit random key
	for (int i = 0; i < 16; i++)
	{
		if (rand() % 2 == 0)
			Key += '0';
		else Key += '1';
	}
	cout<<"Minimax......"<<endl;
	MiniMax(argc,argv);

	//Calculate the binary representation of the watermark.
	for (int i = 0; i < Watermark.size(); i++)
	{
		string temp;
		int w = Watermark[i];
		while (w)
		{
			temp += (char)(w % 2 + '0');
			w /= 2;
			l += 1;
		}
		while (temp.size() < 8)
		{
			temp += '0';
			l += 1;
		}
		reverse(temp.begin(), temp.end());
		BinaryWatermark += temp;
	}
	
	if (argc != 3) {
		fprintf(stderr, "param  is :XX ip port\n");
		return -1;
	}
	cout<<"embegging watermark......"<<endl;
	
	for (int i = 0; i < 32561; i++)
	{
		EmbedWatermark(argc, argv, int2str(i+1));
	}
	
	cout<<"extracting watermark......"<<endl;
	ExtractWatermark(argc, argv);
	double ber = ExtractBer();
	cout << "The bit error rate : " << ber << endl;

}

