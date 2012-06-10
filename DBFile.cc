#include <iostream>
#include "stdio.h"
#include <cstdlib>
#include "string.h"
#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"


bool isOrderMakerPrinted = false;

void GenericDBFile::RemoveFile () {
    remove(fileName);
    remove(strcat(fileName, "_meta_"));
}


void GenericDBFile::RenameFile (char* file_name) {
    rename(fileName, file_name);
}

int GenericDBFile :: CreateMetaFile(char *f_path, fType f_type, void *startup) {
    
    if (f_path == NULL)
        return 0;
    
    if(f_type != sorted && f_type != heap)
        return 0;
    
    //startup is optional as, it will only be used by the Sorted class and not by Heap.
    
    SortInfo* sortInfo = (SortInfo*)startup;
    //Copying the file name into another variable to be used for modifying
    char* fileName = new char[1000];
    char* metaIdentifier = "_meta_";
    strcat(fileName, f_path);
    strcat(fileName, metaIdentifier);
    
    //Insert code here to create a metafile too, with the name sorted on it.
    //This will create a meta file by the same name as the file with a meta added to it.
    ofstream metaFile(fileName);
    
    if (metaFile!=NULL) {
        
        metaFile<<f_type<<endl;
        
        if (f_type==sorted)
        {
            
            metaFile<<sortInfo->myOrder->numAtts<<endl;
            
            metaFile<<sortInfo->runLength<<endl;
            
            for(int i=0; i<sortInfo->myOrder->numAtts; i++) {
                metaFile<<sortInfo->myOrder->whichAtts[i]<<endl;
                metaFile<<sortInfo->myOrder->whichTypes[i]<<endl;
            }
            
        }
        
    }
    delete fileName;
    return 1;
}

DBFile::DBFile () 
{
    
}


int DBFile::Create (char *f_path, fType f_type, void *startup) 
{
    //Here f_type denotes the file type i.e heap or sorted, so accordingly we instantiate the class variable GenericDBFile.
    if(f_type == sorted)
        genericDBFile = new Sorted(f_path);
    else if(f_type == heap)
        genericDBFile = new Heap();
    
    genericDBFile->Create(f_path, f_type, startup);
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    genericDBFile->Load(f_schema, loadpath);
}


int DBFile::Open (char *f_path) {
    
    char* metaFileName = new char[10000];
    char* metaIdentifier = "_meta_";
    
    strcat(metaFileName, f_path);
    strcat(metaFileName, metaIdentifier);
    
    OrderMaker *orderMaker;
    
    int fileType;
    int runLength = 0;
    
    fstream infile;
    infile.open(metaFileName, ifstream::in);
    
    string str;
    int type;
    
    getline(infile, str);
    fileType = atoi(str.c_str());
    
    if (fileType == sorted) {
        orderMaker = new OrderMaker();
        getline(infile, str);
        orderMaker->numAtts = atoi(str.c_str());
        
        getline(infile, str);
        runLength = atoi(str.c_str());
        
        for (int i = 0; i < orderMaker->numAtts; i++) {
            getline(infile, str);
            orderMaker->whichAtts[i] = atoi(str.c_str());
            
            getline(infile, str);
            type = atoi(str.c_str());
            
            switch (type) {
                case 0: orderMaker->whichTypes[i] = Int;
                    break;
                    
                case 1: orderMaker->whichTypes[i] = Double;
                    break;
                    
                case 2: orderMaker->whichTypes[i] = String;
                    break;
            }
        }
    }
    
    if (fileType == 1) {                                    
        
        genericDBFile = new Sorted(orderMaker, runLength, f_path);
        
    } else {//if (fileType == 0) {                               
        
        genericDBFile = new Heap();
        
    }
    
    genericDBFile->Open(f_path);
    
    delete metaFileName;
}



void DBFile::MoveFirst () {
    genericDBFile->MoveFirst();
}


int DBFile::Close () {
    genericDBFile->Close();
}


void DBFile::Add (Record &rec) {
    genericDBFile->Add(rec);
}



int DBFile::GetNext (Record &fetchme) {
    genericDBFile->GetNext(fetchme);
}



int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    genericDBFile->GetNext(fetchme, cnf, literal);
}



Heap::Heap () 
{
	file = new File();							//The current File object where the pages will be written to or read from
    page = new Page();                          //The current Page object.
    isFileOpen = false;                         //A boolean value that will be set once the File is opened    
    isInReadMode = true;                        //A boolean to state weather the page is in Read mode (if false it's in write mode).    
    currentPageInReadBuffer = -1;				//The current page number in the page object when reading. Initially set to -1.
    currentRecordBeingRead = 0;                 //The record number in current page when reading.
    currentPageBeingWritten = 0;				//The current page number in the page object in write mode. Initially set to 0.
    fileType = heap;							//Type of file (heap, sorted, tree)
}

int Heap::Create (char *f_path, fType f_type, void *startup) 
{
    if (f_path == NULL)
        return 0;
    
    //If there's a file open already close it.
    if (isFileOpen)
        Close();
    
    CreateMetaFile(f_path, f_type, startup);
    
    //Create and open a new file
    file->Open(0, f_path);
    isFileOpen = true;
    isInReadMode=true;
    //Set current page being read as -1
    currentPageInReadBuffer=-1;
    currentRecordBeingRead=0;
    currentPageBeingWritten=0;
}

void Heap::Load (Schema &f_schema, char *loadpath) 
{
    if (!isFileOpen || loadpath == NULL)//Make sure we have the output file open and that the given path for the input file is not null
        return;
    
	Record temp;
	//ComparisonEngine comp;
    
	FILE *tableFile = fopen (loadpath, "r");
    if (tableFile==NULL)//If unsucsessful just return
        return;
    
	while (temp.SuckNextRecord (&f_schema, tableFile) == 1) 
	{
		Add(temp);
	}
}


int Heap::Open (char *f_path) 
{
    //if the file path is not provided just return 0
	if (f_path == NULL)
		return 0;
		
	//If there's a file open already close it.
	if (isFileOpen)
		Close();
	
	//Open the file
	file->Open(1, f_path);
    
    //*Set the file type (should be modified to read from a meta file
    fileType=heap;
	isFileOpen = true;
    isInReadMode = true;
	//Set current page being read as -1
	currentPageInReadBuffer=-1;
    currentRecordBeingRead=0;
    if (file->GetLength()>0)
        currentPageBeingWritten=file->GetLength()-2;
    else
        currentPageBeingWritten=0;
	return 1;
}


void Heap::MoveFirst () 
{
	currentPageInReadBuffer=-1;
	currentRecordBeingRead=0;
}


int Heap::Close () 
{
    
	if (!isInReadMode) //If you're currently writing
	{
		//Write back the current page buffer to the file
		file->AddPage(page, currentPageBeingWritten);
	}
    if (isFileOpen)
        file->Close();
	isInReadMode=true;
	isFileOpen=false;
	currentPageInReadBuffer=-1;
	currentRecordBeingRead=0;
    currentPageBeingWritten=0;
    return 1;
	//page->EmptyItOut(); //not yet sure we need to do this here
}


void Heap::Add (Record &rec) 
{
    if (!isFileOpen)
        return;
    
    if (isInReadMode) //If you are currently reading
	{
        //then read the last page that was written into the buffer and set it to write mode
		page->EmptyItOut();
        if (file->GetLength()>0)
        {
            currentPageBeingWritten=file->GetLength()-2;
            file->GetPage(page,currentPageBeingWritten);
        }
        else
            currentPageBeingWritten=0;
        
        isInReadMode=false;
	}
    
    if (!page->Append(&rec)) //Try writing to the current page in the buffer
    {
        //If it fails, then write the current page back and start a fresh page.
        //***Should use a dirty bit boolean to mark if the page is dirty or not.. 
        //***could save time on Add when you pick up the last page in the file to write into and it was already full
        file->AddPage(page,currentPageBeingWritten);
        currentPageBeingWritten++;
        page->EmptyItOut();
        //Now add to the fresh page
        page->Append(&rec);
    }
}


int Heap::GetNext (Record &fetchme) 
{
	if (!isFileOpen)
		return 0;
    
    if (file->GetLength() == 0)
    {
        return 0;
    }
    
	if (!isInReadMode) //If you're currently writing
	{
        isInReadMode=true;
		//Write back the current page buffer to the file
		file->AddPage(page, currentPageBeingWritten);
		page->EmptyItOut();
		if (file->GetLength()>0 && currentPageInReadBuffer >= (file->GetLength()-1))
		{
			return 0;
		}
		else if (currentPageInReadBuffer == -1)
		{
			currentPageInReadBuffer++;
			currentRecordBeingRead=1;
			file->GetPage(page,currentPageInReadBuffer);
			page->GetFirst(&fetchme);
			currentRecordBeingRead++;
		}
		else
		{
			file->GetPage(page,currentPageInReadBuffer);
			int numberOfRecordsToRead=currentRecordBeingRead;
			while (numberOfRecordsToRead>0) 
			{
				if (!page->GetFirst (&fetchme))
				{
					currentPageInReadBuffer++;
					currentRecordBeingRead=1;
					if (currentPageInReadBuffer >= file->GetLength()-1)
					{
						return 0;
					}
					else
					{
						file->GetPage(page,currentPageInReadBuffer);
					}
				}
				else
				{
					numberOfRecordsToRead--;
				}
			}	
			currentRecordBeingRead++;
		}
	}
	if (!page->GetFirst(&fetchme))
	{
		currentPageInReadBuffer++;
		currentRecordBeingRead=1;
		if (currentPageInReadBuffer >= file->GetLength()-1)
		{
			return 0;
		}
		else
		{
			file->GetPage(page,currentPageInReadBuffer);
			if (!page->GetFirst(&fetchme)) //Additional Check.. might be redundant
				return 0;
		}
	}
	else
	{
		currentRecordBeingRead++;
	}
	return 1;
}


int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
	ComparisonEngine comp;
	int returnVal=GetNext(fetchme);
	while (returnVal==1 && !(comp.Compare(&fetchme,&literal,&cnf) ) )
	{
		returnVal=GetNext(fetchme);
	}
	return(returnVal);
}




//Sorted functions and constructor and stuff
Sorted :: Sorted () 
{
	file = new File();							//The current File object where the pages will be read from
    page = new Page();                          //The current Page object.
    isFileOpen = false;                         //A boolean value that will be set once the File is opened    
    isInReadMode = true;                        //A boolean to state weather the page is in Read mode (if false it's in write mode).    
    currentPageInReadBuffer = -1;				//The current page number in the page object when reading. Initially set to -1.
    currentRecordBeingRead = 0;                 //The record number in current page when reading.
    currentPageBeingWritten = 0;				//The current page number in the page object in write mode. Initially set to 0.
    fileType = sorted;							//Type of file (heap, sorted, tree)
    previousCNFUsed = NULL;
    CNFreceived=false;
}


Sorted :: Sorted (char* file_name) {
    file = new File();							//The current File object where the pages will be written to
    page = new Page();                          //The current Page object.
    isFileOpen = false;                         //A boolean value that will be set once the File is opened    
    isInReadMode = true;                        //A boolean to state weather the page is in Read mode (if false it's in write mode).    
    currentPageInReadBuffer = -1;				//The current page number in the page object when reading. Initially set to -1.
    currentRecordBeingRead = 0;                 //The record number in current page when reading.
    currentPageBeingWritten = 0;				//The current page number in the page object in write mode. Initially set to 0.
    fileType = sorted;							//Type of file (heap, sorted, tree)
    fileName = file_name;
    previousCNFUsed = NULL;
    CNFreceived=false;
}

Sorted ::Sorted (OrderMaker* order, int runSize, char* file_name) 
{
	file = new File();							//The current File object where the pages will be written to
    page = new Page();                          //The current Page object.
    isFileOpen = false;                         //A boolean value that will be set once the File is opened    
    isInReadMode = true;                        //A boolean to state weather the page is in Read mode (if false it's in write mode).    
    currentPageInReadBuffer = -1;				//The current page number in the page object when reading. Initially set to -1.
    currentRecordBeingRead = 0;                 //The record number in current page when reading.
    currentPageBeingWritten = 0;				//The current page number in the page object in write mode. Initially set to 0.
    fileType = sorted;							//Type of file (heap, sorted, tree)
    myOrder = new OrderMaker(*order);
    runLength = runSize;
    fileName = file_name;
    previousCNFUsed = NULL;
    CNFreceived=false;
}


Sorted :: ~Sorted()
{
    delete file;
    delete page;
}

int Sorted::Create (char *f_path, fType f_type, void *startup) 
{
    if (f_path == NULL)
        return 0;
    
    //If there's a file open already close it.
    if (isFileOpen)
        Close();
    
    CreateMetaFile(f_path, f_type, startup);
    
    //Create and open a new file
    file->Open(0, f_path);
    isFileOpen = true;
    isInReadMode=true;
    //Set current page being read as -1
    currentPageInReadBuffer=-1;
    currentRecordBeingRead=0;
    currentPageBeingWritten=0;
    SortInfo* sortInfo = (SortInfo*)startup; 
    myOrder = sortInfo->myOrder;
    runLength = sortInfo->runLength;
    fileName=f_path;
    previousCNFUsed = NULL;
    CNFreceived=false;
}


//Could proabably write an inline function to start a fresh page that takes care of setting the default values.


void Sorted::Load (Schema &f_schema, char *loadpath) 
{
    if (loadpath == NULL)//That the given path for the input file is not null
        return;
    
	Record temp;
	//ComparisonEngine comp;
    
	FILE *tableFile = fopen (loadpath, "r");
    if (tableFile==NULL)//If unsucsessful just return
        return;
    
	while (temp.SuckNextRecord (&f_schema, tableFile) == 1) 
	{
		Add(temp);
	}
    
    if (isInReadMode)
    {
        MoveFirst(); //Reset the file pointer to start if it's in read mode.
    }
    previousCNFUsed = NULL;
    CNFreceived=false;
}


int Sorted::Open (char *f_path) 
{
    //if the file path is not provided just return 0
	if (f_path == NULL)
		return 0;
    fileName = f_path;
	//If there's a file open already close it.
	if (isFileOpen)
		Close();
	
	//Open the file
	file->Open(1, f_path);
    //*Set the file type (should be modified to read from a meta file
    fileType=sorted;
	isFileOpen = true;
    isInReadMode = true;
	//Set current page being read as -1
	currentPageInReadBuffer=-1;
    currentRecordBeingRead=0;
    if (file->GetLength()>0)
        currentPageBeingWritten=file->GetLength()-2;
    else
        currentPageBeingWritten=0;
    previousCNFUsed = NULL;
    CNFreceived=false;
	return 1;
}


void Sorted::MoveFirst () 
{
	currentPageInReadBuffer=-1;
	currentRecordBeingRead=0;
    previousCNFUsed = NULL;
    //CNFreceived=false;
}


int Sorted::Close () 
{
    
	if (!isInReadMode) //If you're currently writing
	{
        //Finish Up
        //input->ShutDown();
        Merge();
	}
    if (isFileOpen)
        file->Close();
	isInReadMode=true;
	isFileOpen=false;
	currentPageInReadBuffer=-1;
	currentRecordBeingRead=0;
    previousCNFUsed = NULL;
    CNFreceived=false;
    return 1;
	//page->EmptyItOut(); //not yet sure we need to do this here
}


void Sorted::Add (Record &rec) 
{
    
    if (isInReadMode) 
    {
        isInReadMode = false;
        input = new Pipe(100);
        output = new Pipe(100);
        bigq = new BigQ(*input, *output, *myOrder, runLength);
        previousCNFUsed = NULL;
        CNFreceived=false;
    }
    //cout<<"it's here now"<<endl;
    input->Insert(&rec);
}


int Sorted::GetNext (Record &fetchme) 
{
    if (!isFileOpen)
		return 0;
    
    if (file->GetLength() == 0)
    {
        return 0;
    }
    
	if (!isInReadMode) //If you're currently writing
	{
        isInReadMode=true;
        Merge();
        MoveFirst();
        page->EmptyItOut();

        currentPageInReadBuffer++;
        currentRecordBeingRead=1;
        file->GetPage(page,currentPageInReadBuffer);
        page->GetFirst(&fetchme);
        currentRecordBeingRead++;
	}
    
	if (!page->GetFirst(&fetchme))
	{
		currentPageInReadBuffer++;
		currentRecordBeingRead=1;
		if (currentPageInReadBuffer >= file->GetLength()-1)
		{
			return 0;
		}
		else
		{
			file->GetPage(page,currentPageInReadBuffer);
			if (!page->GetFirst(&fetchme)) //Additional Check.. might be redundant
				return 0;
		}
	}
	else
	{
		currentRecordBeingRead++;
	}
	return 1;
}


int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    if (file->GetLength() == 0)
    {
        return 0;
    }
    
    ComparisonEngine comp;
    
    if (!CNFreceived)
    {
        CNFreceived=true;
        //first call the Get_Query_Order to get the query order ordermaker based on the CNF and the ordermaker used for sorting.
        
        OrderMaker dummy;
        OrderMaker cnfOrder;
        int returnVal;
        cnf.GetSortOrders_new(cnfOrder, dummy);
        
        //cnfOrder.Print();
        
        commonAtts = GetQueryOrder(queryOrder, cnfOrder);
        //queryOrder.Print();
        //Using the ordermaker, do a binary search to find the records that satisfy the condition by the slightest amount
        previousCNFUsed=&cnf;
        lowerBound =0;
        upperBound =file->GetLength()-2;
        firstMatchFound=false;
        noMoreMatches=false;
        MoveFirst();
    }
    
    if (noMoreMatches)
        return 0;
    
    if (commonAtts > 0) 
    {
        if (!firstMatchFound)
        {
            bool centerIsAMatch=false;
            int numberOfPagesToBeRead=0;
            int middle=0;
            while ((upperBound-lowerBound)>0 && !centerIsAMatch)
            {
                middle=(upperBound+lowerBound)/2;
                page->EmptyItOut();
                file->GetPage(page,middle);
                page->GetFirst(&fetchme);
                int comparisonResult = comp.Compare(&fetchme, &literal, &queryOrder);
                if (comparisonResult<0)
                    lowerBound=middle+1;
//                else if (comparisonResult>0)
//                    upperBound=middle-1;
                else
                    centerIsAMatch=true;
            }
            if (centerIsAMatch)
            {
                numberOfPagesToBeRead=(middle-lowerBound)+1;
            }
            else if ((upperBound-lowerBound) == 0)//Scan the current and next page
            {
                numberOfPagesToBeRead=1;
            }

            int numberOfPagesRead =0;
            currentPageInReadBuffer = lowerBound - 1;
            if (currentPageInReadBuffer < 0)
                currentPageInReadBuffer = 0;
            page->EmptyItOut();
            file->GetPage(page,currentPageInReadBuffer);
            page->GetFirst(&fetchme);
            while(!comp.Compare(&fetchme,&literal,&cnf))
            {
                if (!page->GetFirst(&fetchme))
                {
                    currentPageInReadBuffer++;
                    if (currentPageInReadBuffer >= file->GetLength()-1)
                    {
                        noMoreMatches=true;
                        return 0;
                    }
                    else
                    {
                        file->GetPage(page,currentPageInReadBuffer);
                        numberOfPagesRead++;
                        page->GetFirst(&fetchme);
                    }
                }
            }
//            if (numberOfPagesRead==numberOfPagesToBeRead)
//            {
//                //noMoreMatches=true;
//                //return 0;
//            }
//            else
            {
                while (!comp.Compare(&fetchme,&literal,&cnf))
                {
                    if (!page->GetFirst(&fetchme))
                    {
                        currentPageInReadBuffer++;
                        if (currentPageInReadBuffer >= file->GetLength()-1)
                        {
                            noMoreMatches=true;
                            return 0;
                        }
                        else
                        {
                            file->GetPage(page,currentPageInReadBuffer);
                            page->GetFirst(&fetchme);
                        }
                    }
                }
                //int comparisonValue = comp.Compare(&fetchme, &literal, &queryOrder);
                if (!comp.Compare(&fetchme,&literal,&cnf))
                {
                    //cout<<comparisonValue<<":"<<endl;
                    firstMatchFound = false;
                    return 0;
                }
                firstMatchFound=true;
                return 1;
            }
        }
        else
        {
            if (!page->GetFirst(&fetchme))
            {
                currentPageInReadBuffer++;
                if (currentPageInReadBuffer >= file->GetLength()-1)
                {
                    noMoreMatches=true;
                    return 0;
                }
                else
                {
                    file->GetPage(page,currentPageInReadBuffer);
                    page->GetFirst(&fetchme);
                }
            }
            while (!comp.Compare(&fetchme,&literal,&cnf))// && comp.Compare(&fetchme, &literal, &queryOrder)>0)
            {
                if (!page->GetFirst(&fetchme))
                {
                    currentPageInReadBuffer++;
                    if (currentPageInReadBuffer >= file->GetLength()-1)
                    {
                        noMoreMatches=true;
                        return 0;
                    }
                    else
                    {
                        file->GetPage(page,currentPageInReadBuffer);
                        page->GetFirst(&fetchme);
                    }
                }
            }
            
            if (comp.Compare(&fetchme,&literal,&cnf))
            {
                return 1;
            }
            else
            {
                noMoreMatches=true;
                return 0; 
            }
        }
    }
    else
    {
        int returnVal=GetNext(fetchme);
        while (returnVal==1 && !(comp.Compare(&fetchme,&literal,&cnf) ) )
        {
            returnVal=GetNext(fetchme);
        }
        return returnVal;
    }       
    return 0;
}

void Sorted :: Merge () 
{
    if (!isInReadMode)
    {
        isInReadMode=true;
        input->ShutDown();
        DBFile dbFile;                              //This will represent the new temporary file we create to which
        //we will add the merged records
        ComparisonEngine ceng;
        int comparison;
        
        char* tempFileName = new char[100];
//        sprintf(tempFileName, "sorted.temp", file);     //Here file is the char* object referring to the file associated 
        
        sprintf(tempFileName, "%d.temp", (long)this);
        
        //with the current session. Its defined inside GenericDBFile.
        dbFile.Create (tempFileName, heap, NULL);       //To this we'll add the records as we merge them. And since this calls the Add
        //for the heap, so it'll add it in a sequence we provide.
        
        Record *fileRec;
        Record pipeRec;
        Record *minRec;         //Record objects representing file Record, pipe Record and the min of the two.
        
        bool isFileEmpty, isPipeEmpty;
        
        
        MoveFirst();
        isFileEmpty = false;
        isPipeEmpty = false;
        
        int i=1;
        if(GetNext(*fileRec) == 0)          			 //GetNext will keep fetching next records from the current file instance.
        {
            isFileEmpty = true;
		}
        
        if(!output->Remove(&pipeRec))
            isPipeEmpty = true;
        while(!isPipeEmpty && !isFileEmpty) 
        {
            comparison = ceng.Compare(fileRec, &pipeRec, myOrder);            
            if(comparison > 0)   //this is the case where the fileRec is bigger than the pipeRec so we need to send pipeRec first.
            {
                dbFile.Add(pipeRec);
                if(!output->Remove(&pipeRec))
                    isPipeEmpty = true;
                else
                    ++i;
            }
            else
            {
                dbFile.Add(*fileRec);
                if(!GetNext(*fileRec))
                    isFileEmpty = true;
            }
        }
        
        
        //will exit from the loop if one of the two things are empty, i.e the Pipe or the File, so we need to
        //write the data from the other one into the new DBFile object.
        if (!isPipeEmpty) 
        {
            dbFile.Add(pipeRec);
            while(output->Remove(&pipeRec))
            {
                dbFile.Add(pipeRec);
                ++i;
            }
        }
        
        if (!isFileEmpty)
        {
            dbFile.Add(*fileRec);
            while(GetNext(*fileRec))
            {
                dbFile.Add(*fileRec);
            }
        }
        dbFile.Close();
        output->ShutDown();
        //Call rename on the file object of the DBFile instance created above(rename it to the current files name)
        //We can probably add a private function to the DBFile which will take care of the rename.
        remove(fileName);
        //RenameFile(fileName);
        rename(tempFileName, fileName);

    }
    
}




int Sorted :: GetQueryOrder (OrderMaker& queryOrder, OrderMaker& cnfOrder) {
    
    bool isFound = false;
    
    for (int i=0; i<myOrder->numAtts; i++) {
        
        for (int j = 0; j<cnfOrder.numAtts; j++) {
            
            if (myOrder->whichAtts[i] == cnfOrder.whichAtts[j]) {
                
                isFound = true;
                queryOrder.whichAtts[i] = cnfOrder.whichAtts[j];
                queryOrder.whichTypes[i] = cnfOrder.whichTypes[j];
                queryOrder.numAtts++;
            }
        }
        
        if(!isFound) {                                  //This is because the attributes in the queryOrder must be in the same sequence and without gaps.
            break;
        }
    }
    
    return queryOrder.numAtts;
}

