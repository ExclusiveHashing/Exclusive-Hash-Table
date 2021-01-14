#include <iostream>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <fstream>
#include <functional>
#include <ctime>
#include <map>
#include <set>
#include "BOBHash32.h"
#include "exclusive_hashing.h"
#include <algorithm>
#include <string>
#include <time.h>
#include <thread>
#include <unistd.h>
#include <sys/time.h>
#include <mutex>
using namespace std;
#define d num_per_group
int thread_num;
int ssum = 0;
double lf;
struct oper_array
{
	int sz;
	struct oper
	{
		ID_input p;
		int type; //0 insert   1 search    2 del
	} * a;
	oper_array(int n)
	{
		a = new oper[n + 5];
		sz = 0;
	}
	~oper_array()
	{
		delete[] a;
	}
	oper *begin() { return a; }
	oper *end() { return a + sz; }
	void push_back(ID_input x, int type)
	{
		a[sz++] = (oper){x, type};
	}
	oper get(int index) { return a[index]; }
	int size() { return sz; }
};

set<ID_input> all_id_set;
oper_array *insert_vector;
oper_array *positive_search_vector;
oper_array *negative_search_vector;
oper_array *delete_vector;
MainTable *counter = new MainTable[level_size];
int dectednum, HIT, totalnum;

mutex mutex1;
int failed_num = 0;

inline void set_affinity(unsigned int Thread_id)
{
	cpu_set_t s;
	CPU_ZERO(&s);
	CPU_SET(Thread_id, &s);
	if (sched_setaffinity(0, sizeof(cpu_set_t), &s) == -1)
	{
		puts("error: can not set CPU affinity");
	}
}
int insert_num = 0, access_num = 0, max_access_num = 0, inserted_num = 0;
int insert(ID_input v)
{
	/*
	if (1.0 * inserted_num / (totalnum + stash_size) >= lf)
		return -2;
		*/
	int yy = -1;
	int sum = 0;
	for (int ii = 0; ii < level_size; ++ii)
	{
		int tempnext = 0;
		if (ii != level_size - 1)
			tempnext = ii + 1;
		yy = counter[ii].insert(v, yy, tempnext);
		sum++;
		if (yy == 1 || yy == -1)
			break;
	}
	insert_num++;
	access_num += sum;
	max_access_num = max(max_access_num, sum);
	if (yy == -1)
	{
		size_t sz = __atomic_add_fetch(&failed_num, 1, __ATOMIC_ACQUIRE);
		if (sz > stash_size)
		{
			return -2;
		}
		return -1;
	}
	else
	{
		inserted_num++;
		return 1;
	}
}
int search(ID_input v)
{
	long yy = 0;
	long temp = 0;
	for (int ii = 0; ii < level_size; ii++)
	{
		yy = counter[ii].query(v, yy);
		temp += yy;
		if (temp == 1)
		{
			return 1;
		}
		else if (temp < 0)
		{
			return temp;
		}
		else
			temp = 0;
	}
	return -1;
}
int del(ID_input v)
{
	long yy = 0;
	long temp = 0;
	for (int ii = 0; ii < level_size; ii++)
	{
		yy = counter[ii].del(v, yy);
		temp += yy;
		if (temp == 1)
		{
			return 1;
		}
		else if (temp == -1)
		{
			return -1;
		}
		else
			temp = 0;
	}
	return -1;
}
void do_with_thread(oper_array *ar, unsigned int thread_num)
{
	set_affinity(thread_num);
	int sum = 0, x;
	bool bo = true, bo1 = false;
	timeval start, end;
	gettimeofday(&start, NULL);
	for (int i = 0; i < ar->size(); i++)
	{
		switch (ar->get(i).type)
		{
		case 0:
			if (!bo)
				break;
			x = insert(ar->get(i).p);
			if (x == -2)
				bo = false;
			if (x == 1)
			{
				sum += 1;
			}
			break;
		case 1:
			x = search(ar->get(i).p);
			sum += x > 0 ? x : x + 1;
			break;
		}
	}
	gettimeofday(&end, NULL);
	__atomic_add_fetch(&ssum, sum, __ATOMIC_SEQ_CST);
}
void insert_with_thread(oper_array *ar, unsigned int thread_num)
{
	set_affinity(thread_num);
	int *tmp = new int[ar->size()];
	int sum = 0, x, cnt = 0;
	bool bo = true, bo1 = false;
	timeval start, end;
	gettimeofday(&start, NULL);
	for (int i = 0; i < ar->size(); i++)
	{
		if (!bo)
			break;
		x = insert(ar->get(i).p);
		if (x == -2)
			bo = false;
		if (x == 1)
		{
			sum += 1;
			bo1 = true;
			tmp[cnt++] = i;
		}
	}
	gettimeofday(&end, NULL);
	mutex1.lock();
	ssum += sum;
	if (bo1)
	{
		for (int i = 0; i < cnt; i++)
		{
			positive_search_vector->push_back(ar->get(tmp[i]).p, 1);
			delete_vector->push_back(ar->get(tmp[i]).p, 2);
		}
	}
	mutex1.unlock();
}
void search_with_thread(oper_array *ar, unsigned int thread_num)
{
	set_affinity(thread_num);
	int sum = 0, x, cnt = 0;
	bool bo = true, bo1 = false;
	timeval start, end;
	gettimeofday(&start, NULL);
	for (int i = 0; i < ar->size(); i++)
	{
		x = search(ar->get(i).p);
		sum += x > 0 ? x : x + 1;
	}
	gettimeofday(&end, NULL);
	ssum += sum;
}
void delete_with_thread(oper_array *ar, unsigned int thread_num)
{
	set_affinity(thread_num);
	int sum = 0, x, cnt = 0;
	bool bo = true, bo1 = false;
	timeval start, end;
	gettimeofday(&start, NULL);
	for (int i = 0; i < ar->size(); i++)
	{
		sum += del(ar->get(i).p);
	}
	gettimeofday(&end, NULL);
	ssum += sum;
}
void do_all_with_thread(oper_array *all, unsigned int thread_num, double &t, void f(oper_array *, unsigned int))
{
	thread *th[1024];
	oper_array *ve[1024];
	for (int i = 0; i < thread_num; i++)
		ve[i] = new oper_array(all->size() / thread_num);
	for (int i = 0; i < all->size(); i++)
	{
		ve[i % thread_num]->push_back(all->get(i).p, all->get(i).type);
	}
	timeval start, end;
	gettimeofday(&start, NULL);
	for (int i = 0; i < thread_num; i++)
	{
		th[i] = new thread(f, ve[i], i);
	}
	for (int i = 0; i < thread_num; i++)
	{
		th[i]->join();
	}
	gettimeofday(&end, NULL);
	t = (end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec) / 1000000.0;
	printf("totaltime = %.5lf\n", t);
	for (int i = 0; i < thread_num; i++)
	{
		delete ve[i];
		delete th[i];
	}
}
void read_data(char *filename)
{
	char datafileName[100];
	sprintf(datafileName, filename);
	FILE *fin = fopen(datafileName, "rb");
	ID_input tmp_five_tuple;
	int kkk = 0;
	while (fread(&tmp_five_tuple, 1, 8, fin))
	{
		if (all_id_set.size() == 2 * totalnum)
		{
			break;
		}
		if (kkk % 2 == 1)
		{
			all_id_set.insert(tmp_five_tuple);
		}
		++kkk;
	}
	fclose(fin);
}
int main(int argc, char **argv)
{
	srand(time(0));
	thread_num = atoi(argv[1]);
	w = atoi(argv[2]);
	num_per_group = atoi(argv[3]);
	rate = atoi(argv[4]);
	EHT3 = atoi(argv[5]);
	check_num = atoi(argv[6]);
	char filename[100] = "../data/formatted00.dat";
	filename[23] = '\0';

	if (argc > 7)
	{
		lf = atoi(argv[7]) / 20.0;
		//filename[18]=argv[7][0];
	}

	double x;
	for (int i = 0; i < level_size; i++)
	{
		x += pow(rate, i);
	}
	first_level_size = int(500000 / x);

	totalnum = 0;
	for (int kk = 0; kk < level_size; kk++)
	{
		counter[kk].initial(13, d, int(first_level_size * pow(rate, level_size - 1 - kk) / d), kk, 750);
		totalnum += first_level_size * int(pow(rate, level_size - 1 - kk));
	}

	insert_vector = new oper_array(totalnum * 2);
	positive_search_vector = new oper_array(totalnum * 2);
	negative_search_vector = new oper_array(totalnum * 2);
	delete_vector = new oper_array(totalnum * 2);

	read_data(filename);
	cerr << all_id_set.size() << endl;

	cerr << "total num " << totalnum << " all num " << all_id_set.size() << endl;

	double insert_time, positive_time, negative_time, delete_time;
	int idx = 0;

	for (set<ID_input>::iterator iter = all_id_set.begin(); iter != all_id_set.end(); iter++)
	{
		if (insert_vector->size() < totalnum)
		{
			insert_vector->push_back(*iter, 0);
		}
		else
			negative_search_vector->push_back(*iter, 0);
		/*
		if (rand() % 2)
		{
			positive_search_vector->push_back(*iter, 1);
			insert_vector->push_back(*iter, 0);
			//negative_search_vector->push_back(*iter, 1);
		}
		else
		{
			positive_search_vector->push_back(*iter, 0);
		}
		idx++;
		*/
	}
	printf("insert:\n");
	ssum = 0;
	do_all_with_thread(insert_vector, thread_num, insert_time, insert_with_thread);
	printf("ssum=%d\n", ssum);
	//inserted_num = ssum;

	printf("negative search:\n");
	ssum = 0;
	do_all_with_thread(negative_search_vector, thread_num, negative_time, search_with_thread);
	printf("ssum=%d\n", ssum);
	int negative_access_num = -ssum;

	printf("positive search:\n");
	ssum = 0;
	do_all_with_thread(positive_search_vector, thread_num, positive_time, search_with_thread);
	printf("ssum=%d\n", ssum);

	printf("delete search:\n");
	ssum = 0;
	do_all_with_thread(delete_vector, thread_num, delete_time, delete_with_thread);
	printf("ssum=%d\n", ssum);
	/*
	ssum = 0;
	do_all_with_thread(insert_vector, thread_num, insert_time, do_with_thread);
	printf("ssum=%d\n", ssum);

	ssum = 0;
	do_all_with_thread(positive_search_vector, thread_num, positive_time, do_with_thread);
	printf("ssum=%d\n", ssum);*/

	cout << "d=" << d << endl;
	cout << "w=" << w << endl;
	cout << "rate=" << rate << endl;
	cout << "num_per_group=" << num_per_group << endl;
	cout << "level_size=" << level_size << endl;
	cout << "totalnum=" << (totalnum + stash_size) / d << "*" << d << endl;
	cout << "inserted num=" << ssum << endl;
	cout << "load factor=" << double(inserted_num) / (double(totalnum) + stash_size) << endl;
	cout << "nooffset=" << nooffset << endl;
	cout << "thread num=" << thread_num << endl;
	cout << "try insert num=" << insert_vector->size() << endl;
	cout << "positive search num=" << positive_search_vector->size() << endl;
	cout << "negative search num=" << negative_search_vector->size() << endl;
	cout << "delete num=" << delete_vector->size() << endl;
	cout << "insert Mips=" << insert_vector->size() / (1000000 * insert_time) << endl;
	cout << "positive Mips=" << positive_search_vector->size() / (1000000 * positive_time) << endl;
	cout << "negative Mips=" << negative_search_vector->size() / (1000000 * negative_time) << endl;
	cout << "delete Mips=" << delete_vector->size() / (1000000 * delete_time) << endl;
	cout << "insert average access num=" << 1.0 * access_num / insert_num << endl;
	cout << "insert max access num=" << max_access_num << endl;
	cout << "negative average access num=" << 1.0 * negative_access_num / negative_search_vector->size() << endl;

	int offset_number[32] = {0}, total_counter = 0;

	for (int i = 0; i < level_size; i++)
	{
		for (int j = 0; j < counter[i].m; j++)
		{
			int cnt = 0;
			for (int k = 0; k < 32 - w + 1; k++)
			{
				if (counter[i].offset[j] & (1 << k))
				{
					cnt++;
				}
			}
			offset_number[cnt]++;
			total_counter++;
		}
	}
	printf("offset:\n");
	for (int i = 0; i < 32; i++)
	{
		printf("%lf\n", 1.0 * offset_number[i] / total_counter);
	}

	/*delete insert_vector;
	delete positive_search_vector;
	delete negative_search_vector;
	delete delete_vector;*/

	return 0;
}
