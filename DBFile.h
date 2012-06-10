#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"

typedef enum {heap, sorted, tree} fType;


//pointer to this struct will be sent in order to give the sort order
//and the runlength which will be passed to the BigQ class.
struct SortInfo {
    OrderMaker *myOrder;
    int runLength;
};



class GenericDBFile
{
private:
    friend class DBFile;

protected:
    char* fileName;
    File *file;                                                       //The current File object where the pages will be written to
    Page *page;                                                       //The current Page object.
    bool isFileOpen;                                                  //A boolean value that will be set once the File is opened    
    bool isInReadMode;                                                //A boolean to state weather the page is in Read mode (if false it's in write mode).    
    off_t currentPageInReadBuffer;                                    //The current page number in the page object in read mode. Initially set to 0.
    int currentRecordBeingRead;                                       //Current record on the page being read
    off_t currentPageBeingWritten;                                    //The current page number in the page object in write mode. Initially set to 0.
    int fileType;                                                     //Type of file (heap, sorted, tree)
    
    int CreateMetaFile(char *f_path, fType f_type, void *startup);    //The only non-virtual function in this class, that will be used by both Sorted and Heap to create
                                                                      //their own metadata files.
    
    void RenameFile(char* file_name);                                 //The file will be renamed to the char* value passed to it.
    void RemoveFile();                                                //The file object in this class will be removed.

public:
    virtual int Create (char *fpath, fType file_type, void *startup) = 0;
    virtual int Open (char *fpath) = 0;
    virtual int Close () = 0;
    virtual void Load (Schema &myschema, char *loadpath) = 0;
    virtual void MoveFirst () = 0;
    virtual void Add (Record &addme) = 0;
    virtual int GetNext (Record &fetchme) = 0;
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};



class Heap : public GenericDBFile {
public:
	Heap (); 
    
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
    
	void Load (Schema &myschema, char *loadpath);
    
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};



class Sorted : public GenericDBFile {

    OrderMaker* myOrder;                                    //this instance of the structure above will store the reconstructed object.
    int runLength;
    BigQ* bigq;                                             //This will be initialized when the first time the file goes into the write mode.
    Pipe* input;
    Pipe* output;
    OrderMaker queryOrder;
    CNF *previousCNFUsed;
    int commonAtts;
    int lowerBound;
    int upperBound;
    bool noMoreMatches;
    bool firstMatchFound;
    bool CNFreceived;
    void Merge ();
    int GetQueryOrder (OrderMaker& queryOrder, OrderMaker& cnfOrder);  //This function will accept a reference to the query order maker object, and the input CNF and create a query om. 
    
public:
	Sorted (); 
    Sorted (char* file_name); 
    Sorted (OrderMaker* order, int runSize, char* file_name);
    ~Sorted();
    
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
    
	void Load (Schema &myschema, char *loadpath);
    
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};



class DBFile 
{

    GenericDBFile* genericDBFile;                          //this will hold the object of the specific DBFile, based on the type, and will be used in subsequent operations.
public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
