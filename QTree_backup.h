#include <iostream>
#include <cstring>
#include <map>
#include <set>
#include "DBFile.h"
#include "RelOp.h"
#include "Schema.h"
#include "Pipe.h"
#include "Statistics.h"
using namespace std;

extern	struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern 	struct TableList *tables; // the list of tables and aliases in the query
extern	struct AndList *boolean; // the predicate in the WHERE clause
extern	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern	int distinctFunc;  // 2 if there is a DISTINCT in an aggregate query


/**
 ** Just to make the main file work.
 **/

int WriteOnFile=1;
int printTree=0;

void Optimize()
{
    
    //    Optimizer optimizer;
    //    
    //    optimizer.InsertAlias();
    //    
    //    optimizer.getQueryInfo();		
    //    
    //    optimizer.getOrs();
    //    
    //    optimizer.permute();
    //    
    //    optimizer.logicalToPhysical();		
}


class JoinTable;
enum NodeType {SELECT_FILE, SELECT_PIPE, PROJECT, DISTINCT, SUM, GROUPBY, JOIN, WRITE_OUT};

map<string, JoinTable> Join;                                      //This will store the joins(only the equi-join candidates)
map<string, JoinTable> Select;                                    //This map will only store tables and their select ORs.




class Relation                                                    //This will represent each join, the name itself will be the concatenation of
{        
public:                                                          //all the tables in the join sorted, so that is remains a unique identifier.
    string name;
    vector<string> joinNames;
    long cost;
    long size;
    
    Relation(string n, long s=rand()%56)
    {
        name = n;
        size = s;
    }
    
    ~Relation()
    {
    }
};


struct comp
{
    bool operator()(Relation a, Relation b)
    {
        return a.name<b.name;
    }
};


/**
 This class will represent the Nodes in the tree. 
 **/
class QTreeNode
{
    DBFile dbFile;
public:
    
    NodeType type;
    QTreeNode* left;
    QTreeNode* right;
    Schema* schema;
    CNF* cnf;
    AndList* andList;
    string tableName;
    Record* record;
    int pipeId;
    Function* computerMe;
    OrderMaker* orderMaker;
    RelationalOp* relationalOp;
    
    int inputNumAtts;
    int outputNumAtts;
    Pipe* leftPipe;
    Pipe* rightPipe;
    Pipe* outPipe;
    
    
    QTreeNode();
    ~QTreeNode();
};


class QTree
{
    
    Statistics statsObject;
    
    void Initialize ();
    void SelectOrJoin (OrList* orList);
    
public:
    void BuildTree();
    void CreateJoinOrder();             //In this function itself we create a join order for the tables by calling Statistics again and again.
    
    QTreeNode* root;
    QTree();
    ~QTree();
};



/**
 ** Each object of this will represent either a table or a join(in which case the vector joinName will have 2 things in it)
 ** The aliasName hasnt been decided yet, isJoin means whether this is a join of two tables or 
 **/
class JoinTable
{
public:
    bool isJoin;
    string table;
    string joinedWith;
    string aliasName;
    OrList* tableAndOr;                          //This will be the OrList associated with this join/table, so in case of Join it will be something like
    long cost;                                  //a.x = b.y, and in case of simple select, it will be 
    long size;
    
    JoinTable(bool join, string tableName, string joinTable, OrList* tempOrList)
    {
        isJoin = join;
        table = tableName;
        joinedWith = joinTable;
        tableAndOr = tempOrList;
    }
    
    JoinTable();
    ~JoinTable()
    {
        
    }
};

