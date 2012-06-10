//
//  QueryTree.cc
//  
//
//  Created by Sherin Thomas on 4/29/12.
//  Copyright 2012 __MyCompanyName__. All rights reserved.
//

#include "QTree.h"

void QTree :: Initialize ()
{
    //Explore each of the ORLists and check whether they are suitable for select or for join
    
    struct OrList* orList = boolean->left;
    
    while(orList != NULL)
    {
        SelectOrJoin(orList);
        orList = orList->rightOr;
    }
    
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
        string leftVal(leftOperand->value);
        string rightVal(rightOperand->value);
        
        int leftDot = leftVal.find('.');
        int rightDot = rightVal.find('.');
        
        string leftTableName = leftVal.substr(0, leftDot);
        string rightTablename = rightVal.substr(0, rightDot);
        
        JoinTable leftTable(true, leftTableName, rightTablename, orList);
        JoinTable rightTable(true, rightTablename, leftTableName, orList);
        
        Join.insert(pair<string, JoinTable>(leftTableName, leftTable));
        Join.insert(pair<string, JoinTable>(rightTablename, rightTable));
    }
    else if(leftOperand->code == NAME)
    { 
        string leftVal(leftOperand->value);
        int leftDot = leftVal.find('.');
        string leftTableName = leftVal.substr(0, leftDot);
        
        JoinTable leftTable(false, leftTableName, NULL, orList);
        Select.insert(pair<string, JoinTable>(leftTableName, leftTable));
    }
    
    //After this step we have the tables in the join map and the select map, and later on we can deal with them.
}


/**
 ** Will work off of the tableList.
 **/
void QTree :: CreateJoinOrder ()
{
    vector<set<Relation, comp> > joinOrder;
    
    int index = 0;
    int count = 0;
    
    TableList* tableList = tables;
    set<Relation, comp> singleRelations;
    
    //first insert all the single relations into this set, and then insert that set into the vector
    while(tableList != NULL)
    {
        string name = tableList->tableName;
        Relation rel(name);
        singleRelations.insert(rel);
        tableList = tableList->next;
        count++;
    }
    
    joinOrder.push_back(singleRelations);
    
    //This cycle will create the combinations.
    while(index < count)
    {
        set<Relation, comp> set1;
        set<Relation, comp> first;
        set<Relation, comp> second;
        
        for(int j=0; j<=index/2; j++)
        {
            first = joinOrder[j];
            second = joinOrder[index - j];
        }
        
        set<Relation, comp>::iterator fit;
        set<Relation, comp>::iterator sit;
        
        for(fit=first.begin(); sit != first.end(); fit++)
        {
            for(sit=second.begin(); sit != second.end(); sit++)
            {
                if(fit->name == sit->name)
                    continue;
                
                string newStr = fit->name;
                newStr.append(sit->name);
                sort(newStr.begin(), newStr.end());
                
                long estimate = 0.0;
                
                //TODO: Write code to do the estimate here
                
                Relation newRel(newStr, estimate);
                set1.insert(newRel);
            }
        }
        
        joinOrder.push_back(set1);
        index++;
    }
}









