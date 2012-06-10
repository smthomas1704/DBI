//
//  QueryTree.cc
//  
//
//  Created by Sherin Thomas on 4/29/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#include "QTree.h"
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

string dummyString = "SELECT x FROM x AS p, y AS yy WHERE ";


void QTree ::addAlias(Statistics &s, string copyName)
{
    char name[20];
    char alias[20];
    strcpy(name,aliasToName[copyName].c_str());
    strcpy(alias,copyName.c_str());
    s.CopyRel(name,alias);
}
mapStructure QTree ::generateMinimizedInsert(string combo, map<string, mapStructure>* myMap, int size, AndList *alist, Statistics &original)
{
    mapStructure returnValue(combo);
    returnValue.size=size;
    string *set= new string[size];
    char ** tableNames= new char*[size];
    
    int endPosition, startPosition=0;
    for (int i=0;i<size;i++)
    {
        tableNames[i]=new char[10];
        endPosition=combo.find('#',startPosition);
        set[i]=combo.substr(startPosition,(endPosition-startPosition));
        strcpy(tableNames[i], set[i].c_str());
        startPosition=endPosition+1;
        //cout<<tableNames[i]<<endl;
    }
    
    string single;
    string rest;
    if (size == 1)
    {
        returnValue.left="end";
        returnValue.right="end";
        returnValue.stats = original;
        addAlias(returnValue.stats, set[0]);
        returnValue.numberOfRows=returnValue.stats.Estimate(alist,tableNames,size);
        //returnValue.stats.Apply(alist,tableNames,size);
        //cout<<returnValue.combo<<"="<<returnValue.numberOfRows<<endl;
    }
    else
    {
        double cost = 1;
        for (int i=0;i<size;i++)
        {
            single = set[i]+"#";
            rest="";
            for (int j=0;j<size;j++)
            {
                if (j!=i)
                    rest+=set[j]+"#";
            }
            if (myMap->find(single)!=myMap->end() && myMap->find(rest)!=myMap->end())
            {   
                double singleRows = myMap->find(single)->second.numberOfRows;
                double restRows = myMap->find(rest)->second.numberOfRows;
                //cout<<single<<"="<<singleRows<<endl;
                //cout<<rest<<"="<<restRows<<endl;
                if (i==0 || cost> restRows)
                {
                    cost = restRows;
                    //                    if (singleRows < restRows)
                    //                    {
                    //                        returnValue.left=single;
                    //                        returnValue.right=rest;
                    //                    }
                    //                    else
                    {
                        returnValue.right=single;
                        returnValue.left=rest;
                    }
                    returnValue.stats=myMap->find(rest)->second.stats;
                    addAlias(returnValue.stats, set[i]);
                    returnValue.numberOfRows=returnValue.stats.Estimate(alist,tableNames,size);
                    //returnValue.stats.Apply(alist,tableNames,size);
                }
                //cout<<single<<"  "<<rest<<" "<<i<<endl;
            }
            else 
            {
                cout<<"Combination Not Found!!!! "<<single<<"  "<<rest<<endl;
            }
        }
    }
    cout<<returnValue.combo<<"="<<returnValue.numberOfRows<<endl;
    return returnValue;
}

void QTree ::generateAndInsertCombinations(int size, int position, string* tableNames, int currentTable, int numberOfTables, string concatenation, map<string, mapStructure>* myMap, AndList *alist, Statistics &original)
{
    while(position<=size && (size-position)<=(numberOfTables-currentTable-1) && currentTable<numberOfTables)
    {
        string x=concatenation;
        x+=tableNames[currentTable]+"#";
        if (position!=size)
        {
            generateAndInsertCombinations(size, position+1, tableNames, currentTable+1, numberOfTables, x, myMap, alist, original);
        }
        else 
        {
            mapStructure insert(x);
            //cout<<x<<endl;
            insert = generateMinimizedInsert(x, myMap, size, alist, original);
            (*myMap)[x]=insert;
        }
        currentTable++;
    }
}

map<string, mapStructure>* QTree ::generateMap(string *tableNames, int numberOfTables, AndList *alist, Statistics &original)//Also need the Andlist and the original statistics object
{
    map<string, mapStructure>* returnMap = new map<string, mapStructure>();
    for (int size = 1; size<=numberOfTables; size++)
    {
        generateAndInsertCombinations(size, 1, tableNames, 0, numberOfTables, "", returnMap, alist, original);
    }
    return returnMap;
}

Statistics QTree ::initializeStats()
{
	Statistics s;
    char *relName[] = {"supplier","partsupp","lineitem","orders","customer","nation","region","part"};
    
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);
	s.AddAtt(relName[0], "s_name",10000);
	s.AddAtt(relName[0], "s_address",10000);
	s.AddAtt(relName[0], "s_nationkey",25);
	s.AddAtt(relName[0], "s_phone",10000);
	s.AddAtt(relName[0], "s_acctbal",9955);
	s.AddAtt(relName[0], "s_comment",10000);
    
	
	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey",200000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	s.AddAtt(relName[1], "ps_availqty",9999);
	s.AddAtt(relName[1], "ps_supplycost",99865);
	s.AddAtt(relName[1], "ps_comment",799123);
    
    
	s.AddRel(relName[2],6001215);
	s.AddAtt(relName[2], "l_orderkey",1500000);
	s.AddAtt(relName[2], "l_partkey",200000);
	s.AddAtt(relName[2], "l_suppkey",10000);
	s.AddAtt(relName[2], "l_linenumber",7);
	s.AddAtt(relName[2], "l_quantity",50);
	s.AddAtt(relName[2], "l_extendedprice",933900);
	s.AddAtt(relName[2], "l_discount",11);
	s.AddAtt(relName[2], "l_tax",9);
	s.AddAtt(relName[2], "l_returnflag",3);
	s.AddAtt(relName[2], "l_linestatus",2);
	s.AddAtt(relName[2], "l_shipdate",2526);
	s.AddAtt(relName[2], "l_commitdate",2466);
	s.AddAtt(relName[2], "l_receiptdate",2554);
	s.AddAtt(relName[2], "l_shipinstruct",4);
	s.AddAtt(relName[2], "l_shipmode",7);
	s.AddAtt(relName[2], "l_comment",4501941);
    
	s.AddRel(relName[3],1500000);
	s.AddAtt(relName[3], "o_orderkey",1500000);
	s.AddAtt(relName[3], "o_custkey",99996);
	s.AddAtt(relName[3], "o_orderstatus",3);
	s.AddAtt(relName[3], "o_totalprice",1464556);
	s.AddAtt(relName[3], "o_orderdate",2406);
	s.AddAtt(relName[3], "o_orderpriority",5);
	s.AddAtt(relName[3], "o_clerk",1000);
	s.AddAtt(relName[3], "o_shippriority",1);
	s.AddAtt(relName[3], "o_comment",1478684);
    
	
	s.AddRel(relName[4],150000);
	s.AddAtt(relName[4], "c_custkey",150000);
	s.AddAtt(relName[4], "c_name",150000);
	s.AddAtt(relName[4], "c_address",150000);
	s.AddAtt(relName[4], "c_nationkey",25);
	s.AddAtt(relName[4], "c_phone",150000);
	s.AddAtt(relName[4], "c_acctbal",140187);
	s.AddAtt(relName[4], "c_mktsegment",5);
	s.AddAtt(relName[4], "c_comment",149965);
    
	s.AddRel(relName[5],25);
	s.AddAtt(relName[5], "n_nationkey",25);
	s.AddAtt(relName[5], "n_name",25);
	s.AddAtt(relName[5], "n_regionkey",5);
	s.AddAtt(relName[5], "n_comment",25);
    
	s.AddRel(relName[6],25);
	s.AddAtt(relName[6], "r_regionkey",5);
	s.AddAtt(relName[6], "r_name",5);
	s.AddAtt(relName[6], "r_comment",5);
    
    
	s.AddRel(relName[7],200000);
	s.AddAtt(relName[7], "p_partkey",200000);
	s.AddAtt(relName[7], "p_name",199996);
	s.AddAtt(relName[7], "p_mfgr",5);
	s.AddAtt(relName[7], "p_brand",25);
	s.AddAtt(relName[7], "p_type",150);
	s.AddAtt(relName[7], "p_size",50);
	s.AddAtt(relName[7], "p_container",40);
	s.AddAtt(relName[7], "p_retailprice",20899);
	s.AddAtt(relName[7], "p_comment",127459);
	
    //char *relName[] = {"supplier","partsupp","lineitem","orders","customer","nation","region","part"};
    //    s.CopyRel("supplier","s"); 
    //    s.CopyRel("partsupp","ps");
    //    s.CopyRel("lineitem","l");
    //    s.CopyRel("orders","o");
    //	s.CopyRel("customer","c");
    //    s.CopyRel("customer","c1");
    //    s.CopyRel("customer","c2");
    //    s.CopyRel("nation","n");
    //    s.CopyRel("region","r");
    //    s.CopyRel("part","p");
    
    return s;   
}

string QTree ::getSelectComparison(ComparisonOp* comparisonOp, string table)
{
    string returnString="";
    if (comparisonOp==NULL)
        return returnString;
    if (comparisonOp->left->code != NAME)
        return returnString;
    string temp=comparisonOp->left->value;
    string temp2 = temp.substr(0,temp.find('.'));
    string left = temp;//.substr(temp.find('.')+1);
    if (temp2.compare(table)!=0)
        return returnString;
    
    string right="";
    if (comparisonOp->right->code != NAME)
    {
        right=comparisonOp->right->value;
        if (comparisonOp->right->code == STRING)
        {
            string temp="'";
            right+=temp;
            temp+=right;
            right=temp;
        }
    }   
    else
    {
        temp="";
        temp=comparisonOp->right->value;
        temp2 = temp.substr(0,temp.find('.'));
        right = temp;//.substr(temp.find('.')+1);
        if (temp2.compare(table)!=0)
            return returnString;
    }
    string operand="";
    if (comparisonOp->code==LESS_THAN)
        operand=" < ";
    else if (comparisonOp->code==GREATER_THAN)
        operand=" > ";
    else if (comparisonOp->code==EQUALS)
        operand=" = ";
    returnString = left+operand+right;
    
    return returnString;
}
string QTree ::getSelectOr (OrList* orList, string table)
{
    string returnString="";
    if (orList==NULL)
        return returnString;
    //cout<<"Entered OR ";
    string leftString = getSelectComparison(orList->left, table);
    string rightString = getSelectOr(orList->rightOr, table);
    //cout<<leftString<<rightString<<endl;
    if (leftString.size()==0)
    {
        return returnString;
    }
    else if (orList->rightOr!=NULL && rightString.size()==0)
    {
        return returnString;
    }
    returnString=leftString;
    if (rightString.size()!=0)
    {
        if (leftString.size()!=0)
            returnString+=" OR ";
        returnString+=rightString;
    }
    return returnString;
}
string QTree ::getSelectAnd (AndList* andList, string table)
{
    string returnString="";
    if (andList==NULL)
        return returnString;
    //cout<<"Entered And ";
    string leftString= getSelectOr(andList->left, table);
    string rightString= getSelectAnd(andList->rightAnd, table);
    //cout<<leftString<<rightString<<endl;
    returnString=leftString;
    if (leftString.size()!=0)
    {
        string temp="(";
        leftString+=")";
        temp+=leftString;
        leftString=temp;
        returnString=leftString;
    }
    if (rightString.size()!=0)
    {
        if (leftString.size()!=0)
        {
            returnString+=" AND ";
        }
        returnString+=rightString;
    }
    return returnString;
}

string QTree ::getJoinComparison(ComparisonOp* comparisonOp)
{
    string returnString="";
    if (comparisonOp==NULL)
        return returnString;
    if (comparisonOp->left->code != NAME)
        return returnString;
    string temp=comparisonOp->left->value;
    string temp2 = temp.substr(0,temp.find('.'));
    string left = temp;//.substr(temp.find('.')+1);
    
    string right="";
    if (comparisonOp->right->code != NAME)
    {
        right=comparisonOp->right->value;
        if (comparisonOp->right->code == STRING)
        {
            string temp="'";
            right+=temp;
            temp+=right;
            right=temp;
        }
    }   
    else
    {
        temp="";
        temp=comparisonOp->right->value;
        temp2 = temp.substr(0,temp.find('.'));
        right = temp;//.substr(temp.find('.')+1);
    }
    string operand="";
    if (comparisonOp->code==LESS_THAN)
        operand=" < ";
    else if (comparisonOp->code==GREATER_THAN)
        operand=" > ";
    else if (comparisonOp->code==EQUALS)
        operand=" = ";
    returnString = left+operand+right;
    
    return returnString;
}
string QTree ::getTableName(ComparisonOp* comparisonOp)
{
    string returnString="";
    if (comparisonOp==NULL)
        return returnString;
    if (comparisonOp->left->code != NAME)
        return returnString;
    string temp=comparisonOp->left->value;
    string temp2 = temp.substr(0,temp.find('.'));
    string left;// = temp.substr(temp.find('.')+1);
    string tableName=temp2;
    //    if (temp2.compare(table)!=0)
    //        return returnString;
    
    string right="";
    if (comparisonOp->right->code != NAME)
    {
        return tableName;
    }   
    else
    {
        temp="";
        temp=comparisonOp->right->value;
        temp2 = temp.substr(0,temp.find('.'));
        //right = temp.substr(temp.find('.')+1);
        if (temp2.compare(tableName)!=0)
            return returnString;
        else return tableName;
    }
    
    return returnString;
}
string QTree ::getJoinOr2(OrList* orList)
{
    string returnString="";
    if (orList==NULL)
        return returnString;
    
    string leftString = getJoinComparison(orList->left);
    string rightString = getJoinOr2(orList->rightOr);
    
    if (leftString.size()==0)
    {
        return returnString;
    }
    //    else if (orList->rightOr!=NULL && rightString.size()==0)
    //    {
    //        return returnString;
    //    }
    returnString=leftString;
    if (rightString.size()!=0)
    {
        if (leftString.size()!=0)
            returnString+=" OR ";
        returnString+=rightString;
    }
    return returnString;
    
}
string QTree ::getJoinOr (OrList* orList)
{
    string returnString="";
    if (orList==NULL)
        return returnString;
    //cout<<"Entered OR ";
    string tableName=getTableName(orList->left);
    //cout<<"TableName = "<<tableName<<endl;
    if (tableName.size()!=0)
    {
        string right = getSelectOr(orList->rightOr, tableName);
        if (orList->rightOr==NULL || right.size()!=0)
            return returnString;
        //cout<<"Here"<<endl;
    }
    
    string leftString = getJoinComparison(orList->left);
    string rightString = getJoinOr2(orList->rightOr);
    //cout<<leftString<<rightString<<endl;
    if (leftString.size()==0)
    {
        return returnString;
    }
    //    else if (orList->rightOr!=NULL && rightString.size()==0)
    //    {
    //        return returnString;
    //    }
    returnString=leftString;
    if (rightString.size()!=0)
    {
        if (leftString.size()!=0)
            returnString+=" OR ";
        returnString+=rightString;
    }
    return returnString;
}
string QTree ::getJoinAnd (AndList* andList)
{
    string returnString="";
    if (andList==NULL)
        return returnString;
    //cout<<"Entered And ";
    string leftString= getJoinOr(andList->left);
    string rightString= getJoinAnd(andList->rightAnd);
    //cout<<leftString<<rightString<<endl;
    returnString=leftString;
    if (leftString.size()!=0)
    {
        string temp="(";
        leftString+=")";
        temp+=leftString;
        leftString=temp;
        returnString=leftString;
    }
    if (rightString.size()!=0)
    {
        if (leftString.size()!=0)
        {
            returnString+=" AND ";
        }
        returnString+=rightString;
    }
    return returnString;
}

void QTree :: Initialize ()
{
    Statistics s;
    s= initializeStats();
    int i=0;
    TableList *current = tables;
    while (current!=NULL)
    {
        i++;
        current=current->next;
    }
    string *aliasNames=new string[i];
    string *tableNames=new string[i];
    string concatenation="";
    current = tables;
    i=0;
    aliasToName.clear();
    while (current!=NULL)
    {
        cout<<i<<endl;
        aliasNames[i]=current->aliasAs;
        tableNames[i]=current->tableName;
        current=current->next;
        concatenation+=aliasNames[i]+"#";
        char name[20];
        char alias[20];
        strcpy(name,tableNames[i].c_str());
        strcpy(alias,aliasNames[i].c_str());
        //s.CopyRel(name,alias);
        aliasToName[aliasNames[i] ] = tableNames[i];
        aliasToCNF[aliasNames[i] ] = getSelectAnd(boolean,aliasNames[i]);
        cout<<aliasNames[i]<<"= "<<aliasToCNF[aliasNames[i] ]<<endl;
        
        i++;
    }
    
    joinCNF = getJoinAnd(boolean);
    cout<<"Join CNF"<<"= "<<joinCNF<<endl;
    map<string, mapStructure> *myMap = generateMap(aliasNames, i, boolean, s);
    cout<<"Starting to print"<<endl;
    string top = concatenation;
    cout<<endl;
    stack<string> myStack;
    myStack.push(top);
    vector <string> joinOrder;
    while(!myStack.empty())
    {
        top=myStack.top();
        myStack.pop();
        string left = myMap->find(top)->second.left;
        string right = myMap->find(top)->second.right;
        if (left.compare("end")!=0)
        {   
            myStack.push(right);
            myStack.push(left);
            //cout<<top<<" = "<<left<<" "<<right<<endl;
        }
        else
        {
            //cout<<aliasToName[top.substr(0,top.find('#'))]<<" "<<top<<endl;
            joinOrder.push_back(top.substr(0,top.find('#')));
        }
    }
    
    for (int i=0;i<joinOrder.size();i++)
    {
        cout<<joinOrder[i]<<endl;
    }
    //joinOrder - vector that contains the join order in terms of aliases
    //aliasToName - map from alias to Name
    //aliasToCNF - map from alias to cnf for select
    //joinCNF - contains cnf for all joins
    /******************/

    
    finalFunction_copy = finalFunction; // the aggregate function (NULL if no agg)
    tables_copy = tables; // the list of tables and aliases in the query
    boolean_copy = boolean; // the predicate in the WHERE clause
    groupingAtts_copy = groupingAtts; // grouping atts (NULL if no grouping)
    attsToSelect_copy = attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
    distinctAtts_copy = distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
    distinctFunc_copy = distinctFunc;  // 2 if there is a DISTINCT in an aggregate query

    
    finalFunction = NULL; // the aggregate function (NULL if no agg)
    tables = NULL; // the list of tables and aliases in the query
    boolean = NULL; // the predicate in the WHERE clause
    groupingAtts = NULL; // grouping atts (NULL if no grouping)
    attsToSelect = NULL; // the set of attributes in the SELECT (NULL if no such atts)
    
    
    //Explore each of the ORLists and check whether they are suitable for select or for join
//    struct AndList *parseTree = boolean_copy;
//    
//    while(parseTree!=NULL)
//    {
//        struct OrList *por = parseTree->left;           //Coz on the left side of a AndList node, there is an OrList node, and on the right
//        
//        SelectOrJoin(por);                              //side, there are more AndList nodes.
//        
//        parseTree = parseTree->rightAnd;
//    }

    BuildTree(joinOrder);
    PrintTree(root);
    
    Execute (root);    
    
    cout<<"Finished Executing\n";
    
    Record dummy;
    
    int count = 0;
    while(root->outPipe->Remove(&dummy))
    {
        count++;
    }
    
    cout<<"FINAL COUNT: "<<count<<endl;
}



void QTree :: SelectOrJoin (OrList* orList)
{
    ComparisonOp* comp = orList->left;
    
    int leftCode = comp->code;
    
    Operand* leftOperand = comp->left;
    
    Operand* rightOperand = comp->right;
    
    //this means this is a join expression, and they can be put into the join map(separately)
    if((comp->code == EQUALS) && (leftOperand->code == NAME && rightOperand->code ==NAME))
    {
        cout<<"JOIN: "<<"\t";
        PrintOrList(orList);
        cout<<endl;
        
        string leftVal(leftOperand->value);
        
        string rightVal(rightOperand->value);
        
        int leftDot = leftVal.find('.');
        
        int rightDot = rightVal.find('.');
        
        string leftTableName = leftVal.substr(0, leftDot);
        
        string rightTablename = rightVal.substr(0, rightDot);
        
        string leftAttribute = leftVal.substr(leftDot+1, leftVal.size());
        
        string rightAttribute = rightVal.substr(rightDot+1, rightVal.size());
        
        AndInfo leftTable(leftTableName, leftAttribute, orList);
        
        AndInfo rightTable(rightTablename, rightAttribute, orList);
        
        
        if(Join_Map.find(leftTableName) == Join_Map.end())
        {
            vector<AndInfo> tempVec;
            
            tempVec.push_back(leftTable);
            
            Join_Map.insert(pair<string, vector<AndInfo> >(leftTableName, tempVec));
        }
        else
            Join_Map[leftTableName].push_back(leftTable);
        
        if(Join_Map.find(rightTablename) == Join_Map.end())
        {
            vector<AndInfo> tempVec;
            
            tempVec.push_back(rightTable);
            
            Join_Map.insert(pair<string, vector<AndInfo> >(rightTablename, tempVec));
        }
        else
            Join_Map[rightTablename].push_back(rightTable);
        
    }
    else if(leftOperand->code == NAME)
    { 
        
        cout<<"SELECT: "<<"\t";
        PrintOrList(orList);
        cout<<endl;
        
        string leftVal(leftOperand->value);
        
        string rightVal(rightOperand->value);
        
//        cout<<"SELECT: "<<leftVal<<"\t"<<rightVal<<endl;
        
        int leftDot = leftVal.find('.');
        
        string leftTableName = leftVal.substr(0, leftDot);
        
        string leftAttribute = leftVal.substr(leftDot+1, leftVal.size());
        
        AndInfo tabInfo(leftTableName, leftAttribute, orList);
        
        Select_Map[leftTableName].push_back(tabInfo);
    }
    //After this step we have the tables in the join map and the select map, and later on we can deal with them.
}


/**
 *  This function will create the Build Tree.
 **/
void QTree :: BuildTree(vector<string> joinOrder)
{
//    vector<string> joinOrder = GetJoinOrder();
    
    int size = joinOrder.size();
    
    QTreeNode* selectLeft = GetSelectFileNode(joinOrder[0]);
    
    QTreeNode* selectRight;
    
    if(tables_copy->next != NULL)
        selectRight = GetSelectFileNode(joinOrder[1]);
    else
    {
        root = selectLeft;
    }
    
    Record rec;
    int count = 0;
    
    
    int i=2;
    
    for( ;i<=size; i++)
    {
        
        root = GetJoinNode(selectLeft, selectRight);
        
        root->left = selectLeft;
        
        root->right = selectRight;
        
        if(i == size)
            break;
        
        selectRight = GetSelectFileNode(joinOrder[i]);
        
        selectLeft = root;
    }
    
    //Here the remaining nodes are being added to the tree.
    if(finalFunction_copy == NULL)
    {
        QTreeNode* temp = GetProjectNode(root);
        temp->left = root;
        root = temp;
        
        if(distinctAtts_copy==1)
        {
            QTreeNode* distTemp = GetDistincNode(root);
            distTemp->left = root;
            root = distTemp;
        }
    }
    else
    {
        if(groupingAtts == NULL)
        {
            QTreeNode* sumTemp = GetSumNode(root);
            sumTemp->left = root;
            root = sumTemp;
        }
        else
        {
            QTreeNode* groupTemp = GetGroupByNode(root);
            groupTemp->left = root;
            root = groupTemp;
            
            
            if(attsToSelect_copy!=NULL)
            {
                QTreeNode* prjtTemp = GetProjectNode(root);
                prjtTemp->left = root;
                root = prjtTemp;
            }
        }
    }
    
}


void QTree :: Execute (QTreeNode* root)
{
    
    cout<<root->nodeType<<endl;
    
    if(root->left != NULL) 
    {
        Execute(root->left);
        
        root->leftPipe = root->left->outPipe;
    }
    
    if(root->right != NULL)
    {
        Execute(root->right);
        
        root->rightPipe = root->right->outPipe;
    }
    
    root->Execute();
    
    return;
}


void QTree :: PrintTree (QTreeNode* root)
{
    if(root->left != NULL)
        PrintTree(root->left);
    
    if(root->right != NULL)
        PrintTree(root->right);
    
    int type = root->nodeType;
    
    switch(type)
    {
        case SELECT_FILE: cout<<"SELECT"<<endl;
            break;
            
        case PROJECT:  cout<<"PROJECT"<<endl;
            break;
            
        case JOIN:  cout<<"JOIN"<<endl;
            break;
            
        case DISTINCT: cout<<"DISTINCT"<<endl;
            break;
            
        case SUM:  cout<<"SUM"<<endl;
            break;
            
        case GROUPBY: cout<<"GROUPBY"<<endl;
            break;
            
    }
    
    return;
}



QTreeNode* QTree :: GetSelectFileNode(string alias)
{
    cout<<"\nSELECT\n";
    
    string tableName = aliasToName[alias];
    
    cout<<"ALIAS: "<<alias<<"\tTABLE: "<<tableName<<endl;
    
    QTreeNode* selectNode = new SelectFileNode();
    
    selectNode->pipeId = pipeId++;
    
    selectNode->tableName = tableName;
    
    //tableName will only be used in this place.
    selectNode->schema = GetSchema(tableName);
    
    cout<<selectNode->schema->numAtts<<"\t"<<selectNode->schema->fileName<<endl;
    
    selectNode->andList = boolean_copy;
    
    selectNode->cnf = new CNF();
    
    Record *l = new Record();
    
    AndList* andList = new AndList();
    
    AndList* head = andList;
    
    string queryStr = dummyString+aliasToCNF[alias];
    
    cout<<"ALIAS CNF:  "<<alias<<"\t"<<queryStr<<endl;
    
    boolean = NULL;
    
    if(aliasToCNF[alias].compare("")!=0)
    {
    
        char* input = (char*)queryStr.c_str();
        yy_scan_string(input);
        yyparse();
        head = boolean;
    }
    else
    {
        head = NULL;
    }
    
    PrintAndList(head);
    
    cout<<endl;
    
    if(head != NULL)
    {
        cout<<"HEAD NOT NULL\n";
        selectNode->cnf->GrowFromParseTree(head, selectNode->schema, *l); //predicate is the AndList
    }
    else
    {
        selectNode->cnf = new CNF();
    }
    
    selectNode->record = l;
    
    return selectNode;
}


QTreeNode* QTree :: GetProjectNode(QTreeNode* node)
{
    cout<<"\nPROJECT\n";
    
    Schema* schema = node->schema;
    
    CNF* cnf = node->cnf;
    
    QTreeNode* projectNode = new ProjectNode();
    
    projectNode->pipeId = pipeId++;
    
    projectNode->cnf = cnf;
    
    //Get total number of atts from the schema
    projectNode->numAttsInput = schema->GetNumAtts();
    
    Attribute* attrlist = schema->GetAtts();
    
    projectNode->numAttsOutput=0;
    
    struct NameList *nameList=attsToSelect_copy;
    
    //Get the total number of output atts here
    while(nameList!=NULL)
    {
        projectNode->numAttsOutput++;
        
        nameList=nameList->next;
    }
    
    //checking if sum is involved
    int sumflag=0;
    
    if(schema->Find("SUM")!=-1)
    {
        projectNode->numAttsOutput++;
        
        sumflag=1;
    }
    
    cout<<"Out Atts: "<<projectNode->numAttsOutput<<endl;
    
    cout<<"In Atts: "<<projectNode->numAttsInput<<endl;
    
    NameList *name=attsToSelect_copy;
    
    Attribute* schemaattr=new Attribute[projectNode->numAttsOutput];
    
    projectNode->keepMe = new int[projectNode->numAttsOutput];
    
    //First if there is a SUM involved in the schema, add it to the attribute list
    int index=0;
    
    if(sumflag == 1)
    {
        schemaattr[0].myType=Double;
        
        schemaattr[0].name="SUM";
        
        projectNode->keepMe[0] = 0;
        
        index=1;
    }
    
    //check each of the names against the ones in the attributes to see which of them will qualify and add them to the final list
    while(name!=NULL)
    {
        int i=0;
        
        for(;i<projectNode->numAttsInput;i++)
        {
//            cout<<name->name<<"\t"<<attrlist[i].name<<endl;
            
            if(strcmp(name->name,attrlist[i].name)==0)
            {
                projectNode->keepMe[index] = i;
                
                schemaattr[index].name=name->name;
                
                schemaattr[index].myType=attrlist[i].myType;
                
                index++;
                
                break;
            }
        }
        name=name->next;
    }    
    
    cout<<schemaattr[0].name<<endl;
    
    cout<<"WORKING TILL HERE\n";
    
    projectNode->schema=new Schema("catalog",projectNode->numAttsOutput,schemaattr);
    
    return projectNode;
}


QTreeNode* QTree :: GetSumNode(QTreeNode* node)
{
    cout<<"\nSUM\n";
    
    QTreeNode* sumNode = new SumNode();
    
    sumNode->pipeId = pipeId;
        
    cout<<"Getting computeMe\n";
    
    sumNode->computeMe = new Function();
    
    
    char* leftOperand = finalFunction_copy->leftOperand->value;
    string leftString(leftOperand);
    int dot = leftString.find('.');
    leftString = leftString.substr(dot+1, leftString.size());
    
    finalFunction_copy->leftOperand->value = (char*)leftString.c_str();
    
    cout<<finalFunction_copy->leftOperand->value<<endl;
    
    
    sumNode->computeMe->GrowFromParseTree(finalFunction_copy, *(node->schema));
    
    sumNode->computeMe->Print();
    
    //creating the schema for sum
    ofstream fout("sum_schema");
    
    fout<<"BEGIN"<<endl;
    
    fout<<"sum_table"<<endl;
    
    fout<<"sum.tbl"<<endl;
    
    fout<<"SUM Double"<<endl;
    
    fout<<"END";
    
    fout.close();
    
    sumNode->schema=new Schema("sum_schema","sum_table");
    
    remove("sum_schema");
    
    return sumNode;
}


QTreeNode* QTree :: GetSelectPipeNode(QTreeNode* node)
{
    //SelectPipeNode(CNF* selOp, Record literal)
}


QTreeNode* QTree :: GetGroupByNode(QTreeNode* node)
{
    cout<<"\nGROUPBY\n";
    
    //GroupByNode(OrderMaker group, Function compute)
    QTreeNode *groupBy = new GroupByNode();
    
    groupBy->pipeId = pipeId;
    
    groupBy->computeMe =  new Function();
    
    groupBy->computeMe->GrowFromParseTree(finalFunction_copy, *(node->schema));
    
    vector<char*>input;
    
    struct NameList* tempgroup = groupingAtts_copy;
    
    while(tempgroup!=NULL)
    {
        input.push_back(tempgroup->name);
        
        tempgroup=tempgroup->next;
    }
    
    groupBy->andList = CreateAndList(input);
    
    groupBy->cnf = new CNF();
    
    groupBy->record = new Record();
    
    OrderMaker dummy;
    
    groupBy->cnf->GrowFromParseTree(groupBy->andList, node->schema, *(groupBy->record));
    
    groupBy->groupAtts = new OrderMaker;
    
    groupBy->cnf->GetSortOrders(*(groupBy->groupAtts), dummy);
    
    int totatts=input.size();
    
    totatts++;
    
    Attribute * attrlist=new Attribute[totatts];
    
    attrlist[0].myType=Double;
    
    attrlist[0].name="SUM";
    
    tempgroup=groupingAtts;
    
    int i=1;
    
    while(tempgroup!=NULL)
    {
        attrlist[i].name=tempgroup->name;
        
        attrlist[i].myType=node->schema->FindType(tempgroup->name);
        
        i++;
        
        tempgroup=tempgroup->next;
    }
    
    groupBy->schema=new Schema("catalog",totatts,attrlist);
    
    return groupBy;
}


QTreeNode* QTree :: GetJoinNode(QTreeNode* leftNode, QTreeNode* rightNode)
{
    cout<<"\nJOIN\n";
    
    //JoinNode(CNF* selOp, Record literal)
    Schema* leftSchema = leftNode->schema;
    
    Schema* rightSchema = rightNode->schema;
    
    cout<<leftSchema->myAtts[0].name<<endl;
    
    cout<<rightSchema->myAtts[0].name<<endl;
    
    Schema* newSchema = CombineSchema(leftSchema, rightSchema);
    
    QTreeNode* joinNode = new JoinNode();
    
    joinNode->pipeId = pipeId;
    
    joinNode->schema = newSchema;
    
    joinNode->cnf = new CNF();
    
    AndList* andList = new AndList();
    
    AndList* head = andList;
    
    string query = dummyString+joinCNF;
    char* input = (char*)query.c_str();
    yy_scan_string(input);
    boolean = NULL;
    yyparse();
    
    head = boolean;
    
    PrintAndList(head);
    cout<<endl;
    
    Record rec;
    
    joinNode->cnf->GrowFromParseTree(head, joinNode->schema, rec);
    
    cout<<"NO SEG FAULT\n";
    
    return joinNode;
}


QTreeNode* QTree :: GetWriteOutNode(QTreeNode* node)
{
    //WriteOutNode (FILE *o)
}


QTreeNode* QTree :: GetDistincNode(QTreeNode* node)
{
    cout<<"\nDISTINCT\n";
    
    QTreeNode *distinct = new DistinctNode();
    
    distinct->pipeId = pipeId;

    distinct->schema = node->schema;
    
    distinct->cnf = NULL;
    
    distinct->andList = NULL;
    
    return distinct;
}


void QTree :: RemoveTableNameFromOr(OrList* orList)
{
    char* leftoperand = orList->left->left->value;
    
    string leftAttribute(leftoperand);
    
    int leftDot = leftAttribute.find('.');
    
    string leftTableName = leftAttribute.substr(0, leftDot);
    
    string leftAtt = leftAttribute.substr(leftDot+1, leftAttribute.size());
    
    char* rightoperand = orList->left->right->value;
    
    string rightAttribute(rightoperand);
    
    int rightDot = rightAttribute.find('.');
    
    string rightTablename = rightAttribute.substr(0, leftDot);
    
    string rightAtt = rightAttribute.substr(rightDot+1, rightAttribute.size());
    
    orList->left->left->value = (char*)leftAtt.c_str();
    
    if(orList->left->right->code == NAME)
    {
        orList->left->right->value = (char*)rightAtt.c_str();
    }
}



Schema * QTree::GetSchema (string tname)
{
    char* tableName = (char*)tname.c_str();
    
    TableList * temptable;
    
    temptable=tables_copy;
    
    Schema *mySchema;
    
    while(temptable!=NULL)
    {
        if(strcmp(temptable->tableName,tableName)==0)
        {
            cout<<temptable->aliasAs<<"\t"<<tableName<<endl;
            
            mySchema= new Schema ("catalog",temptable->tableName);
            
//            Attribute* attrlist=mySchema->GetAtts();
//             
//            for(int i=0;i<mySchema->GetNumAtts();i++)
//            {
//                char* alias = new char[strlen(tableName) + 1];
//                
//                strcpy(alias,temptable->aliasAs);
//                
//                strcat(alias,".");
//                
//                strcat(alias,attrlist[i].name);
//                
//                attrlist[i].name=alias;
//                
//                cout<<attrlist[i].name<<endl;
//            }

        }
        
        temptable=temptable->next;
    }
    
    cout<<tname<<"\t"<<mySchema->myAtts[0].name<<endl;
    
    return mySchema;
}


string QTree::GetTableName(char* aliasName)
{
    struct TableList* tab = tables_copy;
    
    while(tab != NULL)
    {
        if(strcmp(tab->aliasAs, aliasName) == 0)
            break;
        
        tab= tab->next;
    }
    
    string ret (tab->tableName);
    
    return ret;
}


string QTree::GetAliasName(char* tableName)
{
    struct TableList* tab = tables_copy;
    
    while(tab != NULL)
    {
        if(strcmp(tab->tableName, tableName) == 0)
            break;
        
        tab= tab->next;
    }
    
    string ret (tab->aliasAs);
    
    return ret;
}

Schema* QTree::CombineSchema(Schema *leftSchema, Schema *rightSchema)
{
    Attribute * lattrlist=leftSchema->GetAtts();
    
    Attribute * rattrlist=rightSchema->GetAtts();
    
    int totalatts=leftSchema->GetNumAtts()+rightSchema->GetNumAtts();
    
    Attribute *attrlist=new Attribute[totalatts];
    
    int index;
    
    for(index=0;index<leftSchema->GetNumAtts();index++)
    {
        attrlist[index].myType=lattrlist[index].myType;
        
        attrlist[index].name=lattrlist[index].name;
    }
    
    for(int i=0;i<rightSchema->GetNumAtts();i++,index++)
    {
        attrlist[index].myType=rattrlist[i].myType;
        
        attrlist[index].name=rattrlist[i].name;
    }
    
    Schema *mySchema;
    
    mySchema=new Schema ("catalog",totalatts,attrlist);
    
    return mySchema;
}


OrList* QTree:: CreateOrList(char* input)
{
    OrList* orl=new OrList();
    
    orl->left=new ComparisonOp();
    
    orl->left->code=3;
    
    orl->left->left=new Operand();
    
    orl->left->right=new Operand();
    
    orl->left->left->code=4;
    
    orl->left->left->value=input;
    
    orl->left->right->code=4;
    
    orl->left->right->value=input;
    
    orl->rightOr=NULL;
    
    return orl;
    
}

AndList* QTree:: CreateAndList( vector<char*> &input)
{
    AndList* predicat=new AndList();
    
    AndList* temppointer=predicat;
    
    for(int i=0;i<input.size();i++)
    {
        temppointer->left=CreateOrList(input[i]);
        
        if(i < input.size()-1)
        {
            temppointer->rightAnd=new AndList();
            
            temppointer=temppointer->rightAnd;
        }
        else
            temppointer->rightAnd=NULL;
        
    }
    
    return predicat;
}

vector<string> QTree :: GetJoinOrder()
{
    
}


void SelectFileNode :: Execute()
{
    //DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal   
    
    dbFile.Create((char*)tableName.c_str(), heap, NULL);
    
    relOp->Run(dbFile, *outPipe, *cnf, *record);
}

void JoinNode ::Execute()
{
    //Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal
    
    relOp->Run(*leftPipe, *rightPipe, *outPipe, *cnf, *record);
}

void ProjectNode :: Execute()
{
    //Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput
    
    relOp->Run(*leftPipe, *outPipe, keepMe, numAttsInput, numAttsOutput);
}

void SumNode :: Execute()
{
    //Pipe &inPipe, Pipe &outPipe, Function &computeMe   
    
    relOp->Run(*leftPipe, *outPipe, *computeMe);
}

void GroupByNode :: Execute()
{
    //Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe
    
    relOp->Run(*leftPipe, *outPipe, *groupAtts, *computeMe);
}

void DistinctNode :: Execute()
{
    //Pipe &inPipe, Pipe &outPipe, Schema &mySchema
    
    relOp->Run(*leftPipe, *outPipe, *schema);
}


void QTree :: PrintOperand(struct Operand *pOperand)
{
    if(pOperand!=NULL)
    {
        cout<<pOperand->value<<" ";
    }
    else
        return;
}

void QTree :: PrintComparisonOp(struct ComparisonOp *pCom)
{
    if(pCom!=NULL)
    {
        PrintOperand(pCom->left);
        switch(pCom->code)
        {
            case 1:
                cout<<" < "; break;
            case 2:
                cout<<" > "; break;
            case 3:
                cout<<" = ";
        }
        PrintOperand(pCom->right);
        
    }
    else
    {
        return;
    }
}


void QTree :: PrintOrList(struct OrList *pOr)
{
    if(pOr !=NULL)
    {
        struct ComparisonOp *pCom = pOr->left;
        PrintComparisonOp(pCom);
        
        if(pOr->rightOr)
        {
            cout<<" OR ";
            PrintOrList(pOr->rightOr);
        }
    }
    else
    {
        return;
    }
}


void QTree :: PrintAndList(struct AndList *pAnd)
{
    if(pAnd !=NULL)
    {
        struct OrList *pOr = pAnd->left;
        PrintOrList(pOr);
        if(pAnd->rightAnd)
        {
            cout<<" AND ";
            PrintAndList(pAnd->rightAnd);
        }
    }
    else
    {
        return;
    }
}


