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
        // cout<<tmp_line<<endl;
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

int SingleTest(int bpk, int err, string readf, string writef){

    string command = "rm -rf testdb";
    system(command.c_str());

    // cout<<"inside!"<<endl;
    cout<<endl<<"====Now writing: "<<writef<<" and reading: "<<readf<<endl;

    leveldb::DB* db;
    leveldb::Options options;
    // options.max_open_files *= 16;
    options.create_if_missing = true;    

    string dbpath = "testdb";
    Status status = DB::Open(options,dbpath, &db);    
    // cout<<typeid(adgMod::db).name()<<endl;
    // cout<<&db<<endl;
    cout<<"Status: "<<status.ToString()<<endl;

    assert(status.ok());   

    adgMod::MOD = 5;
    options.filter_policy = leveldb::NewBloomFilterPolicy(bpk);
    adgMod::model_error = err;
    cout<<"Current Error: "<<adgMod::model_error<<endl;
    adgMod::value_size = 64;
    // adgMod::fd_limit = true;
    // adgMod::restart_read = true;


    ReadOptions& read_options = adgMod::read_options;
    WriteOptions& write_options = adgMod::write_options;
    write_options.sync = false;

    // string key = "A";
    // string value = "aaa";
    // string out;

    // db->Put(write_options,key,value);
    // db->Get(read_options,key,&out);

    // cout<<out<<endl;
    
    std::vector<string> keys;
    keys = load_keys(writef);
    adgMod::Stats* instance0 = adgMod::Stats::GetInstance();
    cout<<"====Start Write===="<<endl;

    instance0->StartTimer(0);
    cout<<keys.size()<<endl;
    for(int i=0; i<keys.size(); i++){
        // cout<<i<<endl;
        string value = adgMod::generate_value(0);
        // cout<<value.size();
        db->Put(write_options,keys[i],value);
    }
    instance0->PauseTimer(0);
    cout<<"====Write Time: "<<instance0->ReportTime(0)<<endl;
    cout<<"====End Write===="<<endl;

    VersionSet* cur_set1 = adgMod::db->versions_;
    Version* current1 = cur_set1->current();
    printf("LevelSize %d \n", current1->NumFiles(0));

    // cout << status.ToString() << endl;
    // file_data->Report();
    // cout<<"count:"<<adgMod::db->version_count<<endl;
    // Version* current = adgMod::db->versions_->current();

    // VersionSet* versions = adgMod::db->versions_;
    delete db;
    status = DB::Open(options, dbpath, &db);
    adgMod::db->vlog->Sync();
    adgMod::db->WaitForBackground();
    VersionSet* cur_set = adgMod::db->versions_;
    Version* current = cur_set->current();
    current->PrintAll();
    printf("LevelSize %d \n", current->NumFiles(0));
    cout<<"count:"<<adgMod::db->version_count<<endl;
    cout<<current->learned_index_data_.size()<<endl;

    for (int i = 1; i < config::kNumLevels; ++i) {
        LearnedIndexData::Learn(new VersionAndSelf{current, adgMod::db->version_count, current->learned_index_data_[i].get(), i});
    }
    current->FileLearn();
    adgMod::Stats* instance1 = adgMod::Stats::GetInstance();
    

    std::vector<string> find_keys;
    find_keys = load_keys(readf);
    cout<<find_keys.size()<<" "<<find_keys[2]<<endl;

    cout<<"====Begin Read===="<<endl;
    instance1->StartTimer(1);
    int hit_count = 0;
    int total_count = 0;
    for(int i=0; i<find_keys.size(); i++){
        
        string find_value="";
        // cout<<find_keys[i]<<endl;
        db->Get(read_options,find_keys[i],&find_value);
        total_count++;
        if(find_value.length()>0){
            hit_count ++;
            // cout<<find_keys[i]<<" "<<find_value<<endl;
        }
        else{
            // cout<<"Empty: "<<i<<endl;
            empty_list.push_back(i);
        }
    }
    instance1->PauseTimer(1);
    cout<<"====Read Time: "<<instance1->ReportTime(1)<<endl;
    cout<<"====End Read===="<<endl;


    cout<<"====Write Time: "<<instance0->ReportTime(0)<<endl;
    cout<<"====Read Time: "<<instance1->ReportTime(1)<<endl;
    cout<<"====Hit ratio: "<<hit_count<<" total count: "<<total_count<<endl;


    writetimes.push_back(instance0->ReportTime(0));
    readtimes.push_back(instance1->ReportTime(1));
    ratios.push_back(hit_count);


    delete db;
    return 0;    
}

int main(){

    int data_scale = 100000;
    vector<string> read_file_list;
    // read_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate0_read_" + std::to_string(data_scale) + ".dat");
    // read_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate20_read_" + std::to_string(data_scale) + ".dat");
    // read_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate40_read_" + std::to_string(data_scale) + ".dat");
    // read_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate60_read_" + std::to_string(data_scale) + ".dat");
    // read_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate80_read_" + std::to_string(data_scale) + ".dat");
    read_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate100_read_" + std::to_string(data_scale) + ".dat");


    vector<string> write_file_list;
    // write_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate0_write_" + std::to_string(data_scale) + ".dat");
    // write_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate20_write_" + std::to_string(data_scale) + ".dat");
    // write_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate40_write_" + std::to_string(data_scale) + ".dat");
    // write_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate60_write_" + std::to_string(data_scale) + ".dat");
    // write_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate80_write_" + std::to_string(data_scale) + ".dat");
    write_file_list.push_back("dataset/" + std::to_string(data_scale) + "/random_rate100_write_" + std::to_string(data_scale) + ".dat");

    int err_bound = 8;
    int bpk = 20;



    for(int i=0; i<1; i++){
        cout<<read_file_list[i]<<endl;
        cout<<write_file_list[i]<<endl;
    }

    cout<<"======="<<endl;

    for(int i=0; i<1; i++){
        SingleTest(bpk, err_bound, read_file_list[i], write_file_list[i]);
    }


    cout<<"===Result==="<<endl;
    for(int i=0; i<writetimes.size(); i++){
        cout<<writetimes[i]<<" ";
    }
    cout<<endl;
    for(int i=0; i<readtimes.size(); i++){
        cout<<readtimes[i]<<" ";
    }
    cout<<endl<<"Ratios: ";
    for(int i=0; i<ratios.size(); i++){
        cout<<ratios[i]<<" ";
    }

    ofstream foutw("empty_list.txt");
    for(int i = 0; i < empty_list.size(); i++) {
        foutw << empty_list[i] << endl;
    }

    return 0;    
}
