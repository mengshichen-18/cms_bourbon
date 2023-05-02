#include <cassert>
#include <chrono>
#include <iostream>
#include <unistd.h>
#include <ctime>
#include <iomanip>
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
        // fout<<tmp_line<<endl;
        string tmp = "";
        for (int i = 0; i < tmp_line.size(); ++i) {
            if (tmp_line[i] >= '0' && tmp_line[i] <= '9') {
                tmp = tmp + tmp_line[i];
                }
        }
        // cout<<tmp<<" ";
        // while(tmp.size()>11 && tmp.size()>0){
        //     cout<<"haha"<<endl;
        //     tmp.erase(tmp.size() - 1);
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

    ofstream fout("segexpres.txt", ios::app);
    fout<<std::fixed<<setprecision(0);

    fout<<endl<<"====Now writing: "<<writef<<" and reading: "<<readf<<endl;

    leveldb::DB* db;
    leveldb::Options options;

    options.create_if_missing = true;    

    string dbpath = "testdb";
    Status status = DB::Open(options,dbpath, &db);    
    // fout<<typeid(adgMod::db).name()<<endl;
    // fout<<&db<<endl;
    fout<<"Status: "<<status.ToString()<<endl;

    assert(status.ok());   

    adgMod::MOD = MOD;
    options.filter_policy = leveldb::NewBloomFilterPolicy(bpk);
    adgMod::model_error = err;
    fout<<"Current Error: "<<adgMod::model_error<<endl;
    adgMod::value_size = 64;
    adgMod::string_mode = false;
    adgMod::fd_limit = false;
    adgMod::restart_read = true;


    ReadOptions& read_options = adgMod::read_options;
    WriteOptions& write_options = adgMod::write_options;
    // write_options.sync = false;


    std::vector<string> keys;
    keys = load_keys(writef);

    clock_t  ReadBegin, ReadEnd, WirteBegin, WriteEnd, TrainBegin, TrainEnd;
    double ReadDuration, WriteDuration, TrainDuration;

    fout<<"====Start Write===="<<endl;

    WirteBegin = clock();
    for(int i=0; i<keys.size(); i++){
        string value = adgMod::generate_value(0);
        db->Put(write_options,keys[i],value);
    }
    WriteEnd = clock();
    WriteDuration = WriteEnd - WirteBegin;
    fout<<"====Write Time: "<<WriteDuration<<endl;
    fout<<"====End Write===="<<endl;

    // adgMod::db->vlog->Sync();
    // adgMod::db->WaitForBackground();
    // delete db;
    // status = DB::Open(options, dbpath, &db);
    // adgMod::db->WaitForBackground();
    // cout<<"reopen for iter"<<endl;

    // adgMod::MOD = 5;
    // int total_keys = 0;
    // leveldb::Iterator* it = db->NewIterator(read_options);
    // cout<<it->status().ToString()<<endl;
    // it->SeekToFirst();
    // cout<<it->key().ToString()<<endl;
    // for (; it->Valid(); it->Next()) {
    //     if (!it->Valid()) break;
    //     // cout<<total_keys<<endl;
    //     total_keys++;
    // }

    // delete it;
    // adgMod::MOD = MOD;

    // fout<<"There are "<< total_keys <<" keys in db."<<endl;

    // int BFsize = total_keys * bpk;
    // fout<<"Bloom filter size is "<< BFsize <<" Byte."<<endl;

    adgMod::db->vlog->Sync();
    fout<<"Got sync"<<endl;


    adgMod::db->WaitForBackground();
    fout<<"Gonna delete"<<endl;
    delete db;
    fout<<"deleted"<<endl;
    status = DB::Open(options, dbpath, &db);
    fout<<"Opened"<<endl;
    adgMod::db->WaitForBackground();
    // cout<<"Got bg2"<<endl;
    Version* current = adgMod::db->GetCurrentVersion();
    printf("LevelSize %d %d %d %d %d %d\n", current->NumFiles(0), current->NumFiles(1), current->NumFiles(2), current->NumFiles(3),
                       current->NumFiles(4), current->NumFiles(5));
    // current->PrintAll();


    int seg_num = 0;
    TrainBegin = clock();
    fout<<"Ready to learn!"<<endl;
    if(MOD >= 6){
        for (int i = 1; i < config::kNumLevels; ++i) {
            LearnedIndexData::Learn(new VersionAndSelf{current, adgMod::db->version_count, current->learned_index_data_[i].get(), i});
        }
        cout<<"gonna file learn"<<endl;
        current->FileLearn();
        adgMod::file_data->Report();
        seg_num = adgMod::file_data->Getmodelsize();
        fout<<"There are "<< seg_num <<" segs in learned-index."<<endl;

        int LIsize = seg_num * 4 * 8;
        fout<<"Learned index size is "<< LIsize <<" Byte."<<endl;
    }
    TrainEnd = clock();
    TrainDuration = TrainEnd - TrainBegin;
    fout<<"Train Time: "<< TrainDuration<<endl;


    // cout<<"report2"<<endl;
    // adgMod::file_data->Report();
    // cout<<"end2"<<endl;

    adgMod::db->WaitForBackground();
    delete db;
    status = DB::Open(options, dbpath, &db);
    adgMod::db->WaitForBackground();

    // cout<<"report3"<<endl;
    // current = adgMod::db->GetCurrentVersion();
    // adgMod::file_data->Report();
    // cout<<"end3"<<endl;

    std::vector<string> find_keys;
    find_keys = load_keys(readf);
    fout<<find_keys.size()<<" "<<find_keys[2]<<endl;

    fout<<"====Begin Read===="<<endl;

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
    ReadDuration = (ReadEnd - ReadBegin);
    fout<<"====Read Time: "<<ReadDuration<<endl;
    fout<<"====End Read===="<<endl;

    cout<<"another report"<<endl;
    adgMod::db->WaitForBackground();
    // sleep(10);
    file_data->Report();
    // adgMod::learn_cb_model->Report();
    cout<<"report done"<<endl;


    fout<<"====Write Time: "<<WriteDuration<<endl;
    fout<<"====Read Time: "<<ReadDuration<<endl;
    fout<<"====Hit ratio: "<<hit_count<<" total count: "<<total_count<<endl;
    fout<<"====Train Time: "<<TrainDuration<<endl;


    delete db;
    return 0;    
}

int main(int argc, char * argv[]){

    

    ofstream fout("segexpres.txt", ios::app);
    // fout.open("expres.txt");

    fout<<std::fixed<<setprecision(0);

    fout<<endl<<"======="<<endl;
    char * tmp;


    int err_bound = strtol(argv[1],&tmp,10);
    cout<<err_bound<<endl;
    int bpk = strtol(argv[2],&tmp,10);
    int seg_pc = strtol(argv[6],&tmp,10);
    int MOD = strtol(argv[4],&tmp,10);
    int data_scale = strtol(argv[3],&tmp,10);
    string hit_rate = argv[5];
    
    string read_file;
    read_file = "dataset/seg_" + std::to_string(seg_pc) +"_" + to_string(data_scale) +  "/" + std::to_string(seg_pc) + "%_" + to_string(data_scale) + "_rate" + hit_rate + "_read.txt";

    string write_file;
    write_file = "dataset/seg_" + std::to_string(seg_pc) +"_" + to_string(data_scale) + "/" + std::to_string(seg_pc) + "%_" + to_string(data_scale) + "_write.txt";

    
    fout<<write_file<<endl<<write_file<<endl;

    // fout<<"======="<<endl;

    SingleTest(bpk, err_bound, read_file, write_file, MOD);
    // fout.close();
    
    return 0;    
}
