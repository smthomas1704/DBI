#ifndef QTREE_H
#define QTREE_H

#include <iostream>
#include <cstring>
#include <map>
#include <set>
#include <fstream>
#include <stack>

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

enum NodeType {DEFAULT, SELECT_FILE, SELECT_PIPE, PROJECT, DISTINCT, SUM, GROUPBY, JOIN, WRITE_OUT};

struct mapStructure
{
    int size;
    string combo;
    string left;
    string right;
    double numberOfRows;
    Statistics stats;
    mapStructure(string name)
    {
        combo=name;
        numberOfRows=0;
    }
    mapStructure(const mapStructure& copyMe)
    {
        size=copyMe.size;
        combo=copyMe.combo;
        left=copyMe.left;
        right=copyMe.right;
        numberOfRows=copyMe.numberOfRows;
        stats=copyMe.stats;
    }
    mapStructure()
    {
        
    }
};


class AndInfo
{
public:
    OrList* orList;
    string table;
    string attribute;
    
    AndInfo (string t, string att, OrList* list)
    {
        table = t;
        attribute = att;
        orList = list;
    }
    ~AndInfo()
    {
        
    }
};


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
    
public:
    NodeType nodeType;
    QTreeNode* left;                
    QTreeNode* right;
    Schema* schema;
    CNF* cnf;
    AndList* andList;
    
    string tableName;
    string aliasAs;
    
    Record* record;
    DBFile dbFile;
    int pipeId;
    
    Function* computeMe;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
    OrderMaker* groupAtts;
    
    FILE *outFile;

    Pipe* leftPipe;                 //Only for join both these will be utilized
    Pipe* rightPipe;
    Pipe* outPipe;
    
    virtual void Execute() = 0;
};


class JoinNode : public QTreeNode
{
public:
    
    vector<string> aliasList;
    
    Join* relOp;
    
    void Execute();
    
    JoinNode()
    {
        nodeType = JOIN;
        leftPipe = new Pipe(100);
        rightPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new Join();
        left = NULL;
        right = NULL;
    }
    
    JoinNode(CNF* selOp, Record* literal)
    {
        cnf = selOp;
        record = literal;
        nodeType = JOIN;
        leftPipe = new Pipe(100);
        rightPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new Join();
        left = NULL;
        right = NULL;
    }
    
    ~JoinNode()
    {
        
    }
};


class SelectFileNode : public QTreeNode
{
public:
    
    SelectFile* relOp;
    
    void Execute();
    
    SelectFileNode()
    {
        nodeType = SELECT_FILE;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new SelectFile();
        left = NULL;
        right = NULL;
    }
    
    SelectFileNode(CNF* selOp)
    {
        cnf = selOp;
        nodeType = SELECT_FILE;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new SelectFile();
        left = NULL;
        right = NULL;
    }
    
    ~SelectFileNode()
    {
        
    }
};


class SelectPipeNode : public QTreeNode
{
public:
    
    SelectPipe* relOp;
    
    void Execute();
    
    SelectPipeNode()
    {
        nodeType = SELECT_PIPE;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new SelectPipe();
        left = NULL;
        right = NULL;
    }
    
    SelectPipeNode(CNF* selOp, Record* literal)
    {
        cnf = selOp;
        record = literal;
        nodeType = SELECT_PIPE;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new SelectPipe();
        left = NULL;
        right = NULL;
    }
    
    ~SelectPipeNode()
    {
        
    }
};


class ProjectNode : public QTreeNode
{
public:
    
    Project* relOp;
    
    void Execute();
    
    ProjectNode()
    {
        nodeType = PROJECT;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new Project();
        left = NULL;
        right = NULL;
    }
    
    ProjectNode(int *k, int numAttsI, int numAttsO)
    {
        keepMe = k;
        numAttsInput = numAttsI;
        numAttsOutput = numAttsO;
        nodeType = PROJECT;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new Project();
        left = NULL;
        right = NULL;
    }
    
    ~ProjectNode()
    {
        
    }
};


class SumNode :public QTreeNode
{
public:
    
    Sum* relOp;
    
    void Execute();
    
    SumNode()
    {
        nodeType = SUM;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new Sum(); 
        left = NULL;
        right = NULL;
    }
    
    SumNode(Function* compute)
    {
        computeMe = compute;
        nodeType = SUM;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new Sum();
        left = NULL;
        right = NULL;
    }
    
    ~SumNode()
    {
        
    }
};


class DistinctNode : public QTreeNode
{
public:
    
    DuplicateRemoval* relOp;
    
    void Execute();
    
    DistinctNode()
    {
        nodeType = DISTINCT;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new DuplicateRemoval(); 
        left = NULL;
        right = NULL;
    }
    
    DistinctNode(Schema* mySchema)
    {
        schema = mySchema;
        nodeType = DISTINCT;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new DuplicateRemoval();
        left = NULL;
        right = NULL;
    }
    
    ~DistinctNode()
    {
        
    }
};


class GroupByNode : public QTreeNode
{
public:
    
    GroupBy* relOp;
    
    void Execute();
    
    GroupByNode()
    {
        nodeType = GROUPBY;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new GroupBy();
        left = NULL;
        right = NULL;
    }
    
    GroupByNode(OrderMaker* group, Function* compute)
    {
        groupAtts = group;
        computeMe = compute;
        nodeType = GROUPBY;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new GroupBy();
        left = NULL;
        right = NULL;
    }
    
    ~GroupByNode()
    {
        
    }
};


class WriteOutNode : public QTreeNode
{
    //Pipe &inPipe, FILE *outFile, Schema &mySchema
public:
    
    WriteOut* relOp;
    
    void Execute();
    
    WriteOutNode()
    {
        nodeType = WRITE_OUT;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new WriteOut();
        left = NULL;
        right = NULL;
    }
    
    WriteOutNode (FILE *o)
    {
        outFile = o;
        nodeType = WRITE_OUT;
        leftPipe = new Pipe(100);
        outPipe = new Pipe(100);
        relOp = new WriteOut();
        left = NULL;
        right = NULL;
    }
    
    ~WriteOutNode()
    {
        
    }
};


class QTree
{
    Statistics statsObject; 
    string joinOrder;                   //This will be set by the CreateJoinOrder function, and will be used to make the JoinOrder Tree.
    int pipeId;
    
    void SelectOrJoin (OrList* orList);
    long GetSize(string tableName);     //Returns the size of the table.
    void GetCNFFromOrList(OrList* orList, CNF &cnf);
    int GetInputGetNumAtts();
    int GetOutNumAtts();
    void RemoveTableNameFromOr(OrList* orList);
    AndList* CreateAndList( vector<char*> &input);
    OrList* CreateOrList(char* input);
    vector<string> GetJoinOrder();
    map <string,string> aliasToName;
    map <string,string> aliasToCNF;
    string joinCNF;
    //aliasToCNF["join"];
    
    
public:
    Schema* GetSchema (string tname);
    string GetTableName(char* aliasName);
    string GetAliasName(char* tableName);
    Schema* CombineSchema(Schema *leftSchema, Schema *rightSchema);
    map<string, vector<AndInfo> > Join_Map;
    map<string, vector<AndInfo> > Select_Map;
    
    void BuildTree(vector<string> joinOrder);
    void CreateJoinOrder();             
    void Initialize ();
    void PrintTree (QTreeNode* root);
    void Execute (QTreeNode* root);
    
    
    QTreeNode* GetSelectFileNode(string alias);
    QTreeNode* GetProjectNode(QTreeNode* node);
    QTreeNode* GetSumNode(QTreeNode* node);
    QTreeNode* GetSelectPipeNode(QTreeNode* node);
    QTreeNode* GetGroupByNode(QTreeNode* node);
    QTreeNode* GetJoinNode(QTreeNode* leftNode, QTreeNode* rightNode);
    QTreeNode* GetWriteOutNode(QTreeNode* node);
    QTreeNode* GetDistincNode(QTreeNode* node);
    
    
    struct FuncOperator *finalFunction_copy; // the aggregate function (NULL if no agg)
    struct TableList *tables_copy; // the list of tables and aliases in the query
    struct AndList *boolean_copy; // the predicate in the WHERE clause
    struct NameList *groupingAtts_copy; // grouping atts (NULL if no grouping)
    struct NameList *attsToSelect_copy; // the set of attributes in the SELECT (NULL if no such atts)
    int distinctAtts_copy; // 1 if there is a DISTINCT in a non-aggregate query
    int distinctFunc_copy;  // 2 if there is a DISTINCT in an aggregate query
    
    void PrintAndList(struct AndList *pAnd);
    void PrintOrList(struct OrList *pOr);
    void PrintComparisonOp(struct ComparisonOp *pCom);
    void PrintOperand(struct Operand *pOperand);
    
    void addAlias(Statistics &s, string copyName);
    mapStructure generateMinimizedInsert(string combo, map<string, mapStructure>* myMap, int size, AndList *alist, Statistics &original);
    void generateAndInsertCombinations(int size, int position, string* tableNames, int currentTable, int numberOfTables, string concatenation, map<string, mapStructure>* myMap, AndList *alist, Statistics &original);
    map<string, mapStructure>* generateMap(string *tableNames, int numberOfTables, AndList *alist, Statistics &original);
    Statistics initializeStats();
    string getSelectComparison(ComparisonOp* comparisonOp, string table);
    string getSelectOr (OrList* orList, string table);
    string getSelectAnd (AndList* andList, string table);
    string getJoinComparison(ComparisonOp* comparisonOp);
    string getTableName(ComparisonOp* comparisonOp);
    string getJoinOr2(OrList* orList);
    string getJoinOr (OrList* orList);
    string getJoinAnd (AndList* andList);
    
    QTreeNode* root;
    QTree()
    {
        root = NULL;
        joinOrder = "";
        pipeId = 0;
    }
    ~QTree()
    {
        
    }
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

#endif