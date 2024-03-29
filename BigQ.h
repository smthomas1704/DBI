
#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <cstdlib>
#include <algorithm>
#include <queue>
#include <string>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


class comparer
{
private:
    OrderMaker *sortord;
public:
    comparer(OrderMaker *order);
    bool operator() (Record* left, Record* right);
};


class Run {
private:
    Page topPage;
    friend class Compare;          //This will be used by the priority queue when it will rearrange the objects inside itself.
    
public:
    Record* firstRecord;           //I am making this variable private so that I can expose it through another function, which facilitates updating its value
    OrderMaker* sortord;
    int pageOffset;
    int runSize;
    File *runsFile;
    Run(int runSize, int pageOffset, File *file, OrderMaker* order);
    Run(File *file, OrderMaker* order);
    Run (const Run& run);
    Run& operator= (const Run& run);
    ~Run();
    
    int GetFirstRecord();
};


/**
 ** This class compare was initially written for sort to use, but this was replaced
 ** by the comparer function.
 **/
class Compare {
    OrderMaker *sortord;
public:        
    Compare();
    Compare(OrderMaker *order);
    ~Compare();
    bool operator() (Run* left, Run* right);
}; 

/**
 ** Class BigQ that will do everything
 **/
class BigQ {
private:
    int totalPages;
    bool WriteRunToFile (int runLocation); 
    void AddRunToQueue (int runSize, int pageOffset);
    friend bool comparer (Record* left, Record* right);
    Pipe* inputPipe;
    Pipe* outputPipe;
    pthread_t workerThread;
    char* fileName;
    priority_queue<Run*, vector<Run*>, Compare> runQueue;
    OrderMaker* sortord;

public:
    File runsFile;
    vector<Record*> recordList;
    int runlength;
    
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    BigQ (const BigQ& big);
    BigQ& operator= (const BigQ& big);
    BigQ ();
	~BigQ ();
    
    
    void SortRecordList();
    void MergeRuns ();
    static void *StartMainThread (void *start) {
        BigQ *bigQ = static_cast<BigQ *>(start);
        bigQ->SortRecordList();
        bigQ->MergeRuns();
        //pthread_exit(NULL);
    }
};

#endif
