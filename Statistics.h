#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <iostream>
#include <string>
#include <vector>
#include <map>
using namespace std;


class AttributeStat;
class RelationsStats;


typedef std::map<string, AttributeStat> AttributeMap;
typedef std::map<string, RelationsStats> RelationMap;


class AttributeStat
{
public:
    string attName;
    double distinctTuples;
    
    AttributeStat();
    AttributeStat(string name, int distTuples);
    AttributeStat(const AttributeStat &copyMe);
    AttributeStat operator=(const AttributeStat &copyMe);
    
    char* Serialize();
};

class RelationsStats                                            //A relation can be a single table or a join of two or more relations.
{
public:
    bool isRelationPresent(string relName);
    bool isJoin;
    string relationName;                                        //For individual relations, this name will be the name of the
    int mapSize;
    double numberOfTuples;
    AttributeMap  relAttributes;
    map<string, string> joinRelations;                               //This will store the relations with which this object will be joined.
    
    RelationsStats(string relName, int tuples);
    RelationsStats();
    RelationsStats(const RelationsStats &copyMe);                    
    RelationsStats& operator=(const RelationsStats &copyMe);
    
    char* Serialize();
    void Print();
};

    
class Statistics
{
    int mapSize;
    
    double And(AndList* andList, char** relName, int numJoin);
    double Or(OrList* orList, char** relName, int numJoin);
    double Comparison(ComparisonOp* comparisonOp, char** relName, int numJoin);
    int GetRelationForOperand(Operand* operand, char** relName, int numJoin, RelationsStats &relation);
    
    public:
    RelationMap relationMap;                                     //This will store information for each relation such as number of tuples, distinct tuples and 
                                                                 //so on. Key will be same as relationName
	Statistics();
	Statistics(const Statistics &copyMe);                        //Performs deep copy
    Statistics& operator=(const Statistics &copyMe);
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
    void Print();
    
    bool isRelationInMap(string relName, RelationsStats &relation);
};

#endif
