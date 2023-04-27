#include <cassert>
#include <chrono>
#include <iostream>
#include <ctime>
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

vector<long long int> writetimes;
vector<long long int> readtimes;
vector<long long int> traintimes;
vector<int> ratios;

vector<int> empty_list;

vector<string> load_keys(string filename){
    ifstream file;
    // file.open("keydataset.dat");
    file.open(filename);
    string tmp_line;
    
    vector<string> res;
    while(!file.eof())
    {
        getline(file,tmp_line);
        string tmp = "";
        for (int i = 0; i < tmp_line.size(); ++i) {
            if (tmp_line[i] >= '0' && tmp_line[i] <= '9') {
                tmp = tmp + tmp_line[i];
                }
        }
        // cout<<tmp<<" ";
        // while(tmp.size()<16 && tmp.size()>0){
            
        //     tmp += "0000";
        // }
        // cout<<tmp<<endl;
        tmp_line = tmp;
        if(tmp_line.size()>0)
            res.push_back(tmp_line);
    }
    return res;
}

int SingleTest(int bpk, int err, string readf, string writef, int MOD){

    string command = "rm -rf testdb";
    system(command.c_str());
    cout<<endl<<"====Now writing: "<<writef<<" and reading: "<<readf<<endl;

    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;    

    string dbpath = "testdb";
    Status status = DB::Open(options,dbpath, &db);    
    cout<<"Status: "<<status.ToString()<<endl;

    assert(status.ok());   

    adgMod::MOD = MOD;
    options.filter_policy = leveldb::NewBloomFilterPolicy(bpk);
    adgMod::model_error = err;
    cout<<"Current Error: "<<adgMod::model_error<<endl;
    adgMod::value_size = 64;
    adgMod::file_learning_enabled = true;

    ReadOptions& read_options = adgMod::read_options;
    WriteOptions& write_options = adgMod::write_options;
    // write_options.sync = false;

    
    std::vector<string> keys;
    keys = load_keys(writef);
    cout<<"====Start Write===="<<endl;

    clock_t  ReadBegin, ReadEnd, WirteBegin, WriteEnd, TrainBegin, TrainEnd;
    float ReadDuration, WriteDuration, TrainDuration;


    WirteBegin = clock();
    for(int i=0; i<keys.size(); i++){
        // cout<<i<<endl;
        string value = adgMod::generate_value(0);
        // cout<<value.size();
        db->Put(write_options,keys[i],value);
    }
    WriteEnd = clock();
    WriteDuration = (WriteEnd - WirteBegin); // CLOCKS_PER_SEC;
    cout<<"====Write Time: "<<WriteDuration<<endl;
    cout<<"====End Write===="<<endl;

    adgMod::db->WaitForBackground();
    delete db;
    status = DB::Open(options, dbpath, &db);
    // adgMod::db->vlog->Sync();
    adgMod::db->WaitForBackground();
    Version* current = adgMod::db->GetCurrentVersion();
    // current->PrintAll();
    printf("LevelSize %d %d %d %d %d %d\n", current->NumFiles(0), current->NumFiles(1), current->NumFiles(2), current->NumFiles(3),
                       current->NumFiles(4), current->NumFiles(5));

    TrainBegin = clock();
    if(MOD >= 7){
        for (int i = 1; i < config::kNumLevels; ++i) {
            LearnedIndexData::Learn(new VersionAndSelf{current, adgMod::db->version_count, current->learned_index_data_[i].get(), i});
        }
        current->FileLearn();

        cout<<"Reporting: "<<endl;
        adgMod::file_data->Report();
    }
    TrainEnd = clock();
    TrainDuration = TrainEnd - TrainBegin;
    cout<<"Train Time: "<< TrainDuration<<endl;

    std::vector<string> find_keys;
    find_keys = load_keys(readf);
    cout<<find_keys.size()<<" "<<find_keys[2]<<endl;

    adgMod::db->WaitForBackground();
    delete db;
    status = DB::Open(options, dbpath, &db);
    // adgMod::db->vlog->Sync();
    adgMod::db->WaitForBackground();

    cout<<"====Begin Read===="<<endl;
    int hit_count = 0;
    int total_count = 0;

    ReadBegin = clock();
    for(int i=0; i<find_keys.size(); i++){
        
        string find_value="";
        db->Get(read_options,find_keys[i],&find_value);
        total_count++;
        if(find_value.length()>0){
            hit_count ++;
        }
    }
    ReadEnd = clock();
    ReadDuration = (ReadEnd - ReadDuration); // CLOCKS_PER_SEC;
    cout<<"====Read Time: "<<ReadDuration<<endl;
    cout<<"====End Read===="<<endl;


    cout<<"====Write Time: "<<WriteDuration<<endl;
    cout<<"====Read Time: "<<ReadDuration<<endl;
    cout<<"====Hit ratio: "<<hit_count<<" total count: "<<total_count<<endl;
    cout<<"====Train Time: "<<TrainDuration<<endl;


    writetimes.push_back(WriteDuration);
    readtimes.push_back(ReadDuration);
    ratios.push_back(hit_count);
    traintimes.push_back(TrainDuration);


    delete db;
    return 0;    
}

int main(){

    vector<string> read_file_list;
    read_file_list.push_back("dataset/1000000/random_rate0_read_1000000.dat");
    read_file_list.push_back("dataset/1000000/random_rate20_read_1000000.dat");
    read_file_list.push_back("dataset/1000000/random_rate40_read_1000000.dat");
    read_file_list.push_back("dataset/1000000/random_rate60_read_1000000.dat");
    read_file_list.push_back("dataset/1000000/random_rate80_read_1000000.dat");
    read_file_list.push_back("dataset/1000000/random_rate100_read_1000000.dat");


    vector<string> write_file_list;
    write_file_list.push_back("dataset/1000000/random_rate0_write_1000000.dat");
    write_file_list.push_back("dataset/1000000/random_rate20_write_1000000.dat");
    write_file_list.push_back("dataset/1000000/random_rate40_write_1000000.dat");
    write_file_list.push_back("dataset/1000000/random_rate60_write_1000000.dat");
    write_file_list.push_back("dataset/1000000/random_rate80_write_1000000.dat");
    write_file_list.push_back("dataset/1000000/random_rate100_write_1000000.dat");

    int err_bound = 8;
    int bpk = 20;

    vector<int> err_bound_list;
    err_bound_list.push_back(32);
    err_bound_list.push_back(16);
    err_bound_list.push_back(8);
    err_bound_list.push_back(4);
    err_bound_list.push_back(2);



    for(int i=0; i<6; i++){
        cout<<read_file_list[i]<<endl;
        cout<<write_file_list[i]<<endl;
    }

    cout<<"======="<<endl;

    // Test different empty rate
    for(int i=0; i<6; i++){
        SingleTest(bpk, err_bound, read_file_list[i], write_file_list[i], 7);
    }

    // cout<<"===Result==="<<endl;
    // for(int i=0; i<writetimes.size(); i++){
    //     cout<<writetimes[i]<<" ";
    // }
    // cout<<endl;
    // for(int i=0; i<readtimes.size(); i++){
    //     cout<<readtimes[i]<<" ";
    // }
    // cout<<endl;
    // for(int i=0; i<traintimes.size(); i++){
    //     cout<<traintimes[i]<<" ";
    // }

    for(int i=0; i<6; i++){
        SingleTest(bpk, err_bound, read_file_list[i], write_file_list[i], 5);
    }


    //Test different err bound

    // for(int i=0; i<5; i++){
    //     SingleTest(bpk, err_bound_list[i], read_file_list[3], write_file_list[3]);
    // }


    cout<<"===Result==="<<endl;
    for(int i=0; i<writetimes.size(); i++){
        cout<<writetimes[i]<<" ";
    }
    cout<<endl;
    for(int i=0; i<readtimes.size(); i++){
        cout<<readtimes[i]<<" ";
    }
    cout<<endl;
    for(int i=0; i<traintimes.size(); i++){
        cout<<traintimes[i]<<" ";
    }

    return 0;    
}
