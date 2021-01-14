#ifndef _CMSKETCH_H_VERSION_2
#define _CMSKETCH_H_VERSION_2
#include <sstream>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <fstream>
#include <functional>
#include <ctime>
#include <map>
#include <set>
#include <bitset>
#include <mutex>
#include "BOBHash32.h"
#include "omp.h"
#define MAXINT 2147483647
#define stash_size 8
#define level_size 7
#define calshortprint(fingerprint, offsett) (fingerprint >> offsett) & ((1 << w) - 1)
using namespace std;
int match = 0;
int w;
int num_per_group;
int rate;
int EHT3;
int check_num;
int first_level_size;
class ID_input
{
public:
	char x[26] = {0};
};
bool operator<(ID_input an, ID_input bn)
{
	for (int i = 0; i < 26; i++)
	{
		if (bn.x[i] < an.x[i])
		{
			return true;
		}
		else if (bn.x[i] > an.x[i])
		{
			return false;
		}
	}
	return false;
}
bool occupied[16][500000] = {0};
int changenum[16][500000] = {0};
int load[16][500000] = {0};
mutex mtx[16][500000];
mutex mtx1[16][500000];
int rcount[16] = {0};
int maxchangenum = 0;
class shortFingerprint
{
public:
	unsigned int fingerprint;
};
int nooffset = 0;
int noplace = 0;
shortFingerprint fasttable[7][500000] = {0};
class MainTable
{
public:
	int key_len;
	int d;
	int m;
	unsigned int *counters = NULL;
	unsigned int *offset = NULL;
	int *first_offset = NULL;
	BOBHash32 *hash = NULL;
	int id = 0;
	int *fail;
	int *nextpos;
	~MainTable() { clear(); }
	void initial(int kkey_len, int dd, int mm, int idnum, int seed)
	{
		key_len = kkey_len;
		d = dd;
		m = mm;
		id = idnum;
		counters = new unsigned int[m * d];
		fail = new int[m * d];
		nextpos = new int[m * d];
		memset(fail, 0, m * d * sizeof(int));
		memset(counters, 0, m * d * sizeof(unsigned int));
		memset(nextpos, 0, m * d * sizeof(int));
		offset = new unsigned int[m * d / num_per_group];
		for (int i = 0; i < m * d / num_per_group; i++)
			offset[i] = 0xffffffff;
		first_offset = new int[m * d / num_per_group];
		for (int i = 0; i < m * d / num_per_group; i++)
			first_offset[i] = 0;
		hash = new BOBHash32(seed);
	}
	void clear()
	{
		delete counters;
		delete offset;
		delete first_offset;
		delete hash;
		delete fasttable;
		delete fail;
		delete nextpos;
	}
	int insert(ID_input a, int pos, int nextid)
	{
		char *key = a.x;
		unsigned int fingerprint = (hash->run((const char *)key, key_len));
		int index;
		if (pos == -1)
		{
			index = (murmur3_32((const char *)key, key_len)) % ((m - 1) * d);
		}
		else
		{
			index = pos;
		}
		int group_num = index / num_per_group;
		index = group_num * num_per_group;
		int i;
		mtx[id][index].lock();
		if (load[id][index] != d)
		{
			i = index + load[id][index];
			load[id][index]++;
			counters[i] = fingerprint;
			occupied[id][i] = 1;
			fasttable[id][i].fingerprint = (unsigned int)(calshortprint(fingerprint, first_offset[group_num]));
			mtx[id][index].unlock();
			return 1;
		}

		//update offset
		unsigned int shortprint[d + 1] = {0};
		unsigned int longprint[d + 1] = {0};
		for (int j = 0; j < d; j++)
			longprint[j] = counters[index + j];
		longprint[d] = fingerprint;
		for (i = 0; i < 32 - w + 1; i++)
		{
			if (!(offset[group_num] & (1 << i)))
				continue;
			for (int j = 0; j <= d; j++)
				shortprint[j] = (unsigned int)(calshortprint(longprint[j], i));
			for (int j = 0; j < d; j++)
				if (shortprint[j] == shortprint[d])
				{
					offset[group_num] &= ~(1 << i);
					break;
				}
		}
		if (!(offset[group_num] & (1 << first_offset[group_num])))
		{
			++maxchangenum;
			int old_first_offset = first_offset[group_num];
			for (int j = first_offset[group_num] + 1; j + w - 1 < 32; j++)
				if (offset[group_num] & (1 << j))
				{
					first_offset[group_num] = j;
					break;
				}
			if (offset[group_num] & (1 << first_offset[group_num]))
			{
				for (int j = 0; j < d; j++)
					fasttable[id][index + j].fingerprint =
						(unsigned int)(calshortprint(longprint[j], first_offset[group_num]));
			}
			else
			{
				first_offset[group_num] = old_first_offset;
				++nooffset;
				mtx[id][index].unlock();
				return -1;
			}
		}
		mtx[id][index].unlock();

		char c[1000] = {0};
		int gm = group_num * num_per_group;
		for (int cur = 0;; cur++)
		{
			c[cur] = gm % 10;
			gm = gm / 10;
			if (gm == 0)
				break;
		}
		int kkkk = 0;

		if (id == level_size - 1)
		{
			return -1;
		}

		if (EHT3)
		{
			mtx[id][index].lock();
			if (!fail[index])
			{
				fail[index] = 1;
				mtx[id][index].unlock();
				int bestchose = 0;
				int bestway = 10;
				BOBHash32 *hashplus = NULL;
				hashplus = new BOBHash32(1000);
				kkkk = (hashplus->run((const char *)c, key_len));
				for (int current = 0; current < check_num; ++current)
				{
					kkkk = abs(kkkk + d * current) % ((int(m / rate) - 1) * d);
					if (kkkk < num_per_group)
						kkkk = num_per_group;
					kkkk = kkkk / num_per_group;
					kkkk = kkkk * num_per_group;

					if (load[nextid][kkkk] < bestway)
					{
						bestchose = kkkk;
						bestway = load[nextid][kkkk];
					}
				}
				delete hashplus;

				mtx[id][index].lock();
				nextpos[index] = bestchose;
			}
			mtx[id][index].unlock();
		}
		else
		{
			mtx[id][index].lock();
			nextpos[index] = group_num / rate * num_per_group;
			mtx[id][index].unlock();
		}

		return nextpos[index];
	}

	int query(ID_input a, int pos)
	{
		char *key = a.x;
		unsigned int fingerprint = (hash->run((const char *)key, key_len));
		int index;
		if (pos == 0)
		{
			index = (murmur3_32((const char *)key, key_len)) % ((m - 1) * d);
		}
		else
		{
			index = pos;
		}
		int group_num = index / num_per_group;
		index = group_num * num_per_group;

		bool found = false;
		for (int i = index; i < index + load[id][index]; i++)
		{
			if (fasttable[id][i].fingerprint == (unsigned int)(calshortprint(fingerprint, first_offset[group_num])))
			{
				found = true;
				if (counters[i] == fingerprint)
				{
					return 1;
				}
			}
		}
		if (found || load[id][index] != d)
		{
			return -1 - int(found);
		}

		return nextpos[index];
	}
	int del(ID_input a, int pos)
	{
		char *key = a.x;
		unsigned int fingerprint = (hash->run((const char *)key, key_len));
		int index;
		if (pos == 0)
		{
			index = (murmur3_32((const char *)key, key_len)) % ((m - 1) * d);
		}
		else
		{
			index = pos;
		}
		int group_num = index / num_per_group;
		index = group_num * num_per_group;

		bool found = false;
		for (int i = index; i < index + load[id][index]; i++)
		{
			if (fasttable[id][i].fingerprint == (unsigned int)(calshortprint(fingerprint, first_offset[group_num])))
			{
				found = true;
				if (counters[i] == fingerprint)
				{
					occupied[id][i] = false;
					return 1;
				}
			}
		}
		if (found || load[id][index] != d)
		{
			return -1;
		}

		return nextpos[index];
	}
};

#endif