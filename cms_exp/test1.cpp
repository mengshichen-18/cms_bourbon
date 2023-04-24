#include <cassert>
#include <chrono>
#include <iostream>
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

vector<string> load_keys(string filename){
    ifstream file;
    // file.open("keydataset.dat");
    file.open(filename);
    string tmp_line;
    vector<string> res;
    while(!file.eof())
    {
        getline(file,tmp_line);
        if(tmp_line.size()==0) break;
        res.push_back(tmp_line);
    }
    return res;
}

int main(int argc, char* argv[]){

    string command = "rm -rf testdb";
    system(command.c_str());

    leveldb::DB* db;
    leveldb::Options option;

    int num_pairs_base = 1000;
    int mix_base = 20;

    int num_operations, num_iteration, num_mix;
    float test_num_segments_base;
    float num_pair_step;
    string db_location, profiler_out, input_filename, distribution_filename, ycsb_filename;
    bool print_single_timing, print_file_info, evict, unlimit_fd, use_distribution = false, pause, use_ycsb = false;
    bool change_level_load, change_file_load, change_level_learning, change_file_learning;
    int load_type, insert_bound, length_range;
    string db_location_copy;

    num_operations *= num_pairs_base;
    db_location_copy = db_location;

    adgMod::fd_limit = unlimit_fd ? 1024 * 1024 : 1024;
    adgMod::restart_read = true;
    adgMod::level_learning_enabled ^= change_level_learning;
    adgMod::file_learning_enabled ^= change_file_learning;
    adgMod::load_level_model ^= change_level_load;
    adgMod::load_file_model ^= change_file_load;
    adgMod::MOD = 7;
    

    option.filter_policy = leveldb::NewBloomFilterPolicy(20);
    adgMod::model_error = 8;
    cout<<"Current Error: "<<adgMod::model_error<<endl;
    adgMod::value_size = 128;


    ReadOptions& read_options = adgMod::read_options;
    WriteOptions& write_options = adgMod::write_options;
    
    option.create_if_missing =  true;
    string dbpath = "testdb";
    leveldb::Status status = leveldb::DB::Open(option,dbpath,&db);
    assert(status.ok());


    string key = "A";
    string value = "aaa";
    string out;

    db->Put(write_options,key,value);
    db->Get(read_options,key,&out);

    Version* current = adgMod::db->versions_->current();
    cout << "Starting up5" << endl;
    printf("LevelSize %d %d %d %d %d %d\n", current->NumFiles(0), current->NumFiles(1), current->NumFiles(2), current->NumFiles(3),
                       current->NumFiles(4), current->NumFiles(5));

    return 0;    
}