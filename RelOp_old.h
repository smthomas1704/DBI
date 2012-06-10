#ifndef REL_OP_H
#define REL_OP_H

#include <pthread.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "BigQ.h"


/**
 * This class will be internally used by the other classes here 
 **/
class Input {
public://Mostly int's
    Pipe *inPipeL;
    Pipe *inPipeR;
    Pipe *outPipe;
    DBFile *inFile;
    CNF *selOp;
    Record *literal;
    int* keepMe; //Array
    int numAttsInput; //Regular int
    int numAttsOutput; //Regular int
    Schema* mySchema;
    Function* computeMe;
    OrderMaker* groupAtts;
    FILE* outFile; //Should be a pointer
    int N;
    
    Input (DBFile &inF, Pipe &outP, CNF &selO, Record &l) 
    {
        inFile = &inF;
        outPipe = &outP;
        selOp = &selO;
        literal = &l;
    }
    Input (Pipe &inP, Pipe &outP, CNF &selO, Record &l)
    {
        inPipeL = &inP;
        outPipe = &outP;
        selOp = &selO;
        literal = &l;
    }
    
    Input (Pipe &inP, Pipe &outP, int *k, int numAttsI, int numAttsO)
    {
        inPipeL = &inP;
        outPipe = &outP;
        keepMe = k;
        numAttsInput = numAttsI;
        numAttsOutput = numAttsO;
    }
    
    Input (Pipe &inPL, Pipe &inPR, Pipe &outP, CNF &selO, Record &l)
    {
        inPipeL = &inPL;
        inPipeR = &inPR;
        outPipe = &outP;
        selOp = &selO;
        literal = &l;
    }
    
    Input (Pipe &inPL, Pipe &inPR, Pipe &outP, CNF &selO, Record &l, int n)
    {
        inPipeL = &inPL;
        inPipeR = &inPR;
        outPipe = &outP;
        selOp = &selO;
        literal = &l;
        N = n;
    }
    
    Input (Pipe &inP, Pipe &outP, Schema &myS)
    {
        inPipeL = &inP;
        outPipe = &outP;
        mySchema = &myS;
    }
    
    Input (Pipe &inP, Pipe &outP, Function &comp)
    {
        inPipeL = &inP;
        outPipe = &outP;
        computeMe = &comp;
    }
    
    Input (Pipe &inP, Pipe &outP, OrderMaker &groupA, Function &comp)
    {
        inPipeL = &inP;
        outPipe = &outP;
        groupAtts = &groupA;
        computeMe = &comp;
    }
    
    Input (Pipe &inP, FILE *outF, Schema &myS)
    {
        inPipeL = &inP;
        outFile = outF;
        mySchema = &myS;
    }
};

class RelationalOp {
    
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
    
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
    virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () {
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }

};

class SelectPipe : public RelationalOp {
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone () {
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};

class Project : public RelationalOp { 
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone () { 
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};

class Join : public RelationalOp { 
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone (){ 
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};

class DuplicateRemoval : public RelationalOp {
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone () { 
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};

class Sum : public RelationalOp {
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone (){ 
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};

class GroupBy : public RelationalOp {
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone (){ 
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};

class WriteOut : public RelationalOp {
    pthread_t thread;
    int n_pages;                                            //Use the Use_n_Pages 
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone (){ 
        pthread_join (thread, NULL);
    }
    void Use_n_Pages (int n) {
        n_pages = n;
    }
};
#endif
