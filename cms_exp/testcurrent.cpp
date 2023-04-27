#include <cassert>
#include <chrono>
#include <iostream>
#include <unistd.h>
#include "leveldb/db.h"
#include "leveldb/comparator.h"
#include "mod/util.h"
#include "mod/stats.h"
#include "mod/learned_index.h"
#include <cstring>
#include "mod/cxxopts.hpp"
#include <unistd.h>
#include <fstream>
#include <cmath>
#include <random>
#include "db/version_set.h"
using namespace std;
using namespace leveldb;
using namespace adgMod;

int main(int argc, char* argv[]){

    string command = "rm -rf testdb";
    system(command.c_str());
    leveldb::DB* db;
    leveldb::Options option;
    option.create_if_missing =  true;
    // adgMod::MOD = 7;
    option.filter_policy = leveldb::NewBloomFilterPolicy(20);

    ReadOptions& read_options = adgMod::read_options;
    WriteOptions& write_options = adgMod::write_options;

    string dbpath = "testdb";
    leveldb::Status status = leveldb::DB::Open(option,dbpath,&db);
    assert(status.ok());

    string key = "A";
    string value = "aaa";
    string out;

    db->Put(write_options,key,value);
    db->Get(read_options,key,&out);
    cout<<out<<endl;

    Version* current = adgMod::db->GetCurrentVersion();
    cout<<current->NumFiles(0)<<endl;

    return 0;    
}