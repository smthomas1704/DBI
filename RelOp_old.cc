#include "RelOp.h"
#include <pthread.h>

using namespace std;

int bufferSize = 100;
int runlength = 10;
int runlengthR = 10;


void* selectpipe(void* input) {
    Input *in = (Input *) input;
    ComparisonEngine ce;
    Record tempRecord;
    
    while(in->inPipeL->Remove(&tempRecord)) {
        if((ce.Compare(&tempRecord, in->literal, in->selOp)) == 1) {
            in->outPipe->Insert(&tempRecord);
        }
    }
    
    in->outPipe->ShutDown();
}


/**
 * Take the tuples from the input file , apply the CNF and put it back to the outputPipe.
 **/
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    Input *i= new Input(inPipe, outPipe, selOp, literal);
    pthread_create(&thread, NULL, selectpipe,(void *) i);
}




void *selectFile(void* input)
{
    cout<<"Entered this"<<endl;
    Input *inp = (Input *) input;
    ComparisonEngine ce;                                    //Instance of the ComparisonEngine used for select statement.
    Record tempRecord;                                      //Temporary record that needs to be sent to the Compare method of ComparisonEngine.
    
    while(inp->inFile->GetNext(tempRecord,*(inp->selOp),*(inp->literal)) == 1) {
        inp->outPipe->Insert(&tempRecord);
    }
    
    inp->outPipe->ShutDown();
}

/**
 * Take the tuples from the input file , apply the CNF and put it back to the outputPipe.
 **/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) 
{
    Input *i= new Input(inFile, outPipe, selOp, literal);
    pthread_create(&thread, NULL, selectFile,(void *) i);
}


void *project(void* input)
{
    Input *in = (Input *) input;
    Record* inputRecord = new Record();
    
    int count = 0;
    while(in->inPipeL->Remove(inputRecord) == 1) {
        inputRecord->Project(in->keepMe, in->numAttsOutput, in->numAttsInput);
        count++;
        in->outPipe->Insert(inputRecord);
    }
    
    in->outPipe->ShutDown();
}


/**
 * Take the attributes from the array keepMe and add it in that order to the result tuple.
 **/
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    Input *i= new Input(inPipe, outPipe, keepMe, numAttsInput, numAttsOutput);
    pthread_create(&thread, NULL, project,(void *) i);
}



void* duplicate(void* input) {
    
    Input *in = (Input *) input;
    OrderMaker* orderMaker = new OrderMaker(in->mySchema);                      //OrderMaker object based on all the attributes of the table in question
    
    ComparisonEngine ce;
    
    Pipe outputPipe(bufferSize);
    
    BigQ bigq(*(in->inPipeL), outputPipe, *orderMaker, runlength);                   //TODO: Hardcoded runlength, change it to a static variable or sth.
    
    Record *Rec1 = new Record(); Record *Rec2 = new Record();
    ComparisonEngine *CE = new ComparisonEngine();
    
    outputPipe.Remove(Rec1);
    
    while(outputPipe.Remove(Rec2))
    {
        if(CE->Compare(Rec1, Rec2, orderMaker)!=0) //Returns 0 when records are duplicate ..
        {
            in->outPipe->Insert(Rec1);
            Rec1->Copy(Rec2);
        }
    }
    
    if(Rec1->bits != NULL)
    {
        in->outPipe->Insert(Rec1);
    }
    in->outPipe->ShutDown();
}



/**
 * Create a cnf containing all the attributes of the record, and compare each record using this cnf.
 * then insert only one of the kind into the output pipe.
 **/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    
    Input *i= new Input(inPipe, outPipe, mySchema);
    pthread_create(&thread, NULL, duplicate,(void *) i);
}



void* groupby(void* input)
{
    Input *inp = (Input *) input;
    Pipe sortedPipe(bufferSize);
    int intVal, intSum=0;
    double doubleVal, doubleSum=0.0;
    Type mytype;
    int count = 0;
    
    char buffer[100];    
    Record previousRecord;
    Record currentRecord;
    Record outputRecord;
    
    int numAttsLeft = inp->groupAtts->GetNumAtts();
    int* whichAtts = inp->groupAtts->GetWhichAtts();
    int* attsToKeep = new int[numAttsLeft + 1];
    
    attsToKeep[0] = 0;
    
    for(int i=1; i<=numAttsLeft; i++) {
        attsToKeep[i] = whichAtts[i - 1];
    }
    
    
    Attribute iAtt = {"int", Int};                
    Schema iSch("outputSchema", 1, &iAtt); 
    Attribute dAtt = {"double", Double};
    Schema dSch("outputSchema", 1, &dAtt);
    //creta a bigQ to sort the input records.
    BigQ bigq(*(inp->inPipeL), sortedPipe, *(inp->groupAtts), runlength);

    if (sortedPipe.Remove(&previousRecord))        
    {        
        mytype = inp->computeMe->Apply(previousRecord,intVal,doubleVal);        
        if (mytype==Int)            
        {            
            intSum+=intVal;
        }
        else if (mytype==Double)
        {
            doubleSum+=doubleVal;
        }
    }
    else
    {
        inp->outPipe->ShutDown();
        return NULL;
    }

    ComparisonEngine CE;
    while (sortedPipe.Remove(&currentRecord))
    {
        if (CE.Compare(&previousRecord, &currentRecord, inp->groupAtts)==0)//currentRecord==previousRecord
        {
            inp->computeMe->Apply(currentRecord,intVal,doubleVal);
            if (mytype==Int)
            {
                intSum+=intVal;
            }
            else if (mytype==Double)
            {
                doubleSum+=doubleVal;
            }
        }
        else
        {
            if (mytype==Int)                
            {
                sprintf(buffer, "%d|", intSum);               
                outputRecord.ComposeRecord(&iSch, buffer);                
            }            
            else if (mytype==Double)                
            {              
                sprintf(buffer, "%f|", doubleSum);   
                outputRecord.ComposeRecord(&dSch, buffer);
            }
            
            Record merged;
            merged.MergeRecords(&outputRecord, &previousRecord, 1,((int *) previousRecord.bits)[1] / sizeof (int) - 1, attsToKeep, numAttsLeft + 1, 1);
            inp->outPipe->Insert(&merged);

            intSum=0;doubleSum=0.0;            
            inp->computeMe->Apply(currentRecord,intVal,doubleVal);
            if (mytype==Int)                
            {                
                intSum+=intVal;                
            }            
            else if (mytype==Double)                
            {
                doubleSum+=doubleVal;   
            }
            previousRecord.Consume(&currentRecord);
        }
    } 

    if (mytype==Int)                
    {
        sprintf(buffer, "%d|", intSum);               
        outputRecord.ComposeRecord(&iSch, buffer);                
    }            
    else if (mytype==Double)                
    {              
        sprintf(buffer, "%f|", doubleSum);   
        outputRecord.ComposeRecord(&dSch, buffer);
    }

    Record merged;
    merged.MergeRecords(&outputRecord, &previousRecord, 1,((int *) previousRecord.bits)[1] / sizeof (int) - 1, attsToKeep, numAttsLeft + 1, 1);
    inp->outPipe->Insert(&merged);
    
    inp->outPipe->ShutDown();
}



void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) 
{    
    Input *i= new Input(inPipe, outPipe, groupAtts, computeMe);
    pthread_create(&thread, NULL, groupby,(void *) i);
}


/**
 * This function will be used by Join to merge two records from different relations
 **/
Record MergeRecordAttributes (Record &leftRecord, Record &rightRecord) {
    
    int leftNumRecords = leftRecord.GetRecordNumAtts();
    int rightNumRecords = rightRecord.GetRecordNumAtts();
    
    int totalAtts = leftNumRecords + rightNumRecords;                    //total records in the final joined record.
    
    int* attsToKeep = new int[totalAtts];
    
    int tempLeft = leftNumRecords - 1;
    int tempRight = 0;
    
    //In merge remove the att which is common between left and right, keep the one only from left
    while (tempLeft >= 0) {
        attsToKeep[tempLeft] = tempLeft--;
    }
    
    int i = leftNumRecords;
    while (i < totalAtts) {
        attsToKeep[i++] = tempRight++;
    }
    
    Record mergedRecord;
    mergedRecord.MergeRecords (&leftRecord, &rightRecord, leftNumRecords, 
                               rightNumRecords, attsToKeep, totalAtts, leftNumRecords);
    
    return mergedRecord;
}



/**
 * Parameters: output pipe, ordermaker for left, ordermaker for right.
 **/
void SortMergeJoin (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, OrderMaker sortOrderLeft, OrderMaker sortOrderRight) {
    //This is the case where we need to do the sort-merge join.
    
    Pipe leftPipe(bufferSize);
    
    Pipe rightPipe(bufferSize);
    
    BigQ leftBigQ(inPipeL, leftPipe, sortOrderLeft, runlength);
    
    BigQ rightBigQ(inPipeR, rightPipe, sortOrderRight, runlengthR);
    
    //Now we have the sorted inputs from both the output pipes.
    //Check for equality between each record, and if there is then combine them
    //If the left record is lesser than the righ one, advance the left one, otherwise advance the left one.
    //So in the beginning, just store a record from the left one in a temp Record variable, and then compare it with the records coming
    //from the other table
    
    Record leftRecord;
    Record rightRecord;
    
    ComparisonEngine ceng;
    
    bool isContinue = true;
    
    if(leftPipe.Remove(&leftRecord) == 0)
        isContinue = false;
    
    if(rightPipe.Remove(&rightRecord) == 0)
        isContinue = false;
    
    int count = 0;
    while(isContinue)
    {
        while(ceng.Compare(&leftRecord, &sortOrderLeft, &rightRecord, &sortOrderRight) > 0) {
            if(rightPipe.Remove(&rightRecord) == 0) {
                isContinue = false;
                break;
            }
        }
        
        while(ceng.Compare(&leftRecord, &sortOrderLeft, &rightRecord, &sortOrderRight) < 0) {
            if(leftPipe.Remove(&leftRecord) == 0) {
                isContinue = false;
                break;
            }
        }
        
        while(ceng.Compare(&leftRecord, &sortOrderLeft, &rightRecord, &sortOrderRight) == 0) {
            
            Record mergedRecord = MergeRecordAttributes (leftRecord, rightRecord);
            
            count++;
            
            outPipe.Insert(&mergedRecord);
            
            if(rightPipe.Remove(&rightRecord) == 0) {
                isContinue = false;
                break;
            }
        }
    }
    while(leftPipe.Remove(&leftRecord) == 1);
    while(rightPipe.Remove(&rightRecord) == 1);
    outPipe.ShutDown();
}



/** Nested loop function - to be used for nested loop joins**/
void NestedLoopJoin (Pipe &left, Pipe &right, Pipe &outPipe, CNF &selOp, Record &literal, int N) {
    
    char* fileName;
    sprintf(fileName, "%d.tmp", rand()%1900);
    
    DBFile dbFile;
    dbFile.Create(fileName, heap, NULL);
    dbFile.Open(fileName);
    
    Record record;
    
    while(left.Remove(&record)) {
        dbFile.Add(record);
    }
    
    Record rightRecord;
    while(right.Remove(&rightRecord)) {
        
        dbFile.Open(fileName);
        dbFile.MoveFirst();
        Record leftRecord;
        
        while(dbFile.GetNext(leftRecord) == 1) {
            Record merged = MergeRecordAttributes(leftRecord, rightRecord);
            outPipe.Insert(&merged);
        }
        
        dbFile.Close();
    }
    
    outPipe.ShutDown();
}



void *join(void* input)
{
    Input *in = (Input *) input;
    ComparisonEngine ce;                                    //Instance of the ComparisonEngine used for select statement.
    Record tempRecord;                                      //Temporary record that needs to be sent to the Compare method of ComparisonEngine.
    int count = 0;
    
    //Check If the selOp contains an equal, then they can me combined using sort merge, otherwise do a "block nested join"
    OrderMaker sortOrderLeft;								//Initialize sortOrder for left relation
    OrderMaker sortOrderRight;								//Initialize sortOrder for right relation
    
	in->selOp->GetSortOrders(sortOrderLeft, sortOrderRight);

    if((sortOrderLeft.GetNumAtts()>0) && (sortOrderRight.GetNumAtts()>0))
    {
        SortMergeJoin(*(in->inPipeL), *(in->inPipeR), *(in->outPipe), sortOrderLeft, sortOrderRight);
        
    }else{
        NestedLoopJoin(*(in->inPipeL), *(in->inPipeR), *(in->outPipe), *(in->selOp), *(in->literal), in->N);
    }
    
}



/**
 * Function to join two table and all the attributes of all the tuples from both the tables.
 **/
void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    Input *i= new Input(inPipeL, inPipeR, outPipe, selOp, literal);
    pthread_create(&thread, NULL, join,(void *) i);
}



void *sum (void *input)

{
    Input *inp = (Input *) input;
    
    Record inputRecord;
    
    Record outputRecord;
    
    int intVal,intSum=0;
    
    double doubleVal, doubleSum=0;
    
    char buffer[100];
    
    Type mytype;
    
    while (inp->inPipeL->Remove(&inputRecord))
        
    {
        
        mytype = inp->computeMe->Apply(inputRecord, intVal, doubleVal);
        
        if (mytype==Int)
            
        {
            
            intSum+=intVal;
            
        }
        
        else
            
        {
            
            doubleSum+=doubleVal;
            
        }
        
    }
    
    if (mytype==Int)
        
    {
        
        sprintf(buffer, "%d|", intSum);
        
        Attribute att = {"int", Int};
        
        Schema sch("outputSchema", 1, &att);
        
        outputRecord.ComposeRecord(&sch, buffer);
        
    }
    
    else if (mytype==Double)
        
    {
        
        sprintf(buffer, "%f|", doubleSum);
        
        Attribute att = {"double", Double};
        
        Schema sch("outputSchema", 1, &att);
        
        outputRecord.ComposeRecord(&sch, buffer);
        
    }
    
    inp->outPipe->Insert(&outputRecord);
    
    inp->outPipe->ShutDown();
    
}


void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    
    Input *i= new Input (inPipe, outPipe, computeMe);
    
    pthread_create(&thread, NULL, sum,(void *) i);
    
}


void* writeout (void* input) {
    
    Input *in = (Input *) input;
    Record* rec = new Record();
    while(in->inPipeL->Remove(rec))
    {
        rec->WriteOutRecord(in->mySchema, in->outFile);
        delete rec;
        rec = new Record();
    }
    delete rec;
}


void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    Input *i= new Input (inPipe, outFile, mySchema);
    pthread_create(&thread, NULL, writeout,(void *) i);
}



