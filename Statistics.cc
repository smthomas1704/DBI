#include "Statistics.h"
#include <fstream>
#include <cmath>
#include <cstring>

AttributeStat :: AttributeStat ()
{
    
}


AttributeStat :: AttributeStat(string name, int distTuples)
{
    attName = name;
    
    distinctTuples = distTuples;
}

AttributeStat AttributeStat::operator=(const AttributeStat &copyMe)
{
    attName = copyMe.attName;
    
    distinctTuples = copyMe.distinctTuples;
    
    return *this; 
}


AttributeStat :: AttributeStat(const AttributeStat &copyMe)
{
    attName = copyMe.attName;
    
    distinctTuples = copyMe.distinctTuples;
}


char* AttributeStat :: Serialize() 
{

}
    

RelationsStats :: RelationsStats ()
{
    isJoin = false;
}

RelationsStats :: RelationsStats(string relName, int tuples)
{
    relationName = relName;
    
    numberOfTuples = tuples;
    
    isJoin = false;
}


RelationsStats& RelationsStats::operator=(const RelationsStats &copyMe)
{
    isJoin = copyMe.isJoin;
    
    relationName = copyMe.relationName;
    
    numberOfTuples = copyMe.numberOfTuples;
    
    AttributeMap::iterator it;
    
    AttributeMap copyMap = copyMe.relAttributes;
    
    relAttributes.insert(copyMap.begin(), copyMap.end());
    
    return *this;
}


RelationsStats :: RelationsStats(const RelationsStats &copyMe)
{
    isJoin = copyMe.isJoin;
    
    relationName = copyMe.relationName;
    
    numberOfTuples = copyMe.numberOfTuples;    
    
    AttributeMap::iterator it;
    
    AttributeMap copyMap = copyMe.relAttributes;
    
    relAttributes.insert(copyMap.begin(), copyMap.end());

}


void RelationsStats :: Print ()
{
    cout<<relationName<<"\t"<<numberOfTuples<<"\t";
    
    AttributeMap::iterator it;
    
    cout<<"Attributes:\t";
    
    for(it = relAttributes.begin(); it!=relAttributes.end(); it++) {
        
        AttributeStat second = it->second;
        
        cout<<second.attName<<"\t"<<second.distinctTuples<<"\t";
    }
}


bool RelationsStats :: isRelationPresent(string relName)
{
    //either this object itself is the relation or its joinRelation map contains it.
    
    if(relationName == relName)
        return true;
    
    map<string, string>::iterator it;
    
    it = joinRelations.find(relName);
    
    if (it != joinRelations.end())
        return true;
    
    return false;
}

Statistics::Statistics()
{

}


Statistics::Statistics(const Statistics &copyMe)
{
    RelationMap copyMap = copyMe.relationMap;
    
    relationMap.insert(copyMap.begin(), copyMap.end());
}


Statistics& Statistics::operator=(const Statistics &copyMe)
{
    relationMap.insert(copyMe.relationMap.begin(), copyMe.relationMap.end());    
    
    return *this;
}


Statistics::~Statistics()
{
    
}


double Statistics::And(AndList* andList, char** relName, int numJoin)
{
    if(andList == NULL)
        return 1.0;
    
    double leftValue = 1.0;
    
    double rightValue = 1.0;
    
    leftValue = Or(andList->left, relName, numJoin);
    
//    cout<<"Left Value at AND: "<<leftValue<<endl; 
    
    rightValue = And(andList->rightAnd, relName, numJoin);
    
//    cout<<"Right Value at AND: "<<rightValue<<endl;
//    cout<<"Return value from AND: "<<(leftValue*rightValue)<<endl;
    return ((double)leftValue*rightValue);
}


double Statistics::Or(OrList* orList, char** relName, int numJoin)
{
    if(orList == NULL)
        return 0.0;
    
    double orLeftComp = 0.0;
    
    double orRightValue = 0.0;
    
    double orFactor = 0.0;
    
    //calculate the left ComparisonOp of the OrList;
    orLeftComp = Comparison(orList->left, relName, numJoin);
    
    int count = 1;
    
    //check if the OR, is comparing the same attribute with multiple values, in which case we can use the estimate of the first one multiplied by the
    //number of times it will be ORed, because the number of times it will be ORed increases its probablity of appearing in the final result.
    
    OrList* orRightTemp = orList->rightOr;
    
    char* orAtt = orList->left->left->value;
    
    while(orRightTemp)
    {
        if(strcmp(orRightTemp->left->left->value, orAtt) == 0)
            count++;
        
        orRightTemp = orRightTemp->rightOr;
    }
    
    if(count>1)
        return (double)count*orLeftComp;
    
    orRightValue = Or(orList->rightOr, relName, numJoin);
    
//    cout<<"OR Left Comp: "<<orLeftComp<<endl;
//    cout<<"OR Right Value: "<<orRightValue<<endl;
    
    orFactor = (double)(1.0 - ((1.0 - orLeftComp)*(1.0 - orRightValue)));
    
//    cout<<"OrFactor: "<<orFactor<<endl;
//    cout<<"Return from OR: "<<orFactor<<endl;
    return orFactor;
}


double Statistics::Comparison(ComparisonOp* comparisonOp, char** relName, int numJoin)
{
    //This function will take a look at each operand and the operation on the left and on the right and return the correct factor.
    //There is a left operand and a right operand and a code that represents the operation, like equal, not equal and so on..
    
    RelationsStats leftRelation;
    
    RelationsStats rightRelation;
    
    double distinctLeft = 0.0, distinctRight = 0.0;
    
    double factor = 0.0;
    
    //if -1 is returned that means there was some error
    int leftResult = GetRelationForOperand(comparisonOp->left, relName, numJoin, leftRelation);
    
    int rightResult = GetRelationForOperand(comparisonOp->right, relName, numJoin, rightRelation);
    
    int code = comparisonOp->code;
    
    if(comparisonOp->left->code == NAME)
    {
        if(leftResult < 0)
        {
            cout<<comparisonOp->left->value<<" not present in any relation"<<endl;
            distinctLeft=(double)1.0;
            //return 1.0;
        }
        else
        {
            distinctLeft = leftRelation.relAttributes[comparisonOp->left->value].distinctTuples;
        }
        
    } 
    else 
    {
        distinctLeft = (double)-1.0;
    }
    
    if (comparisonOp->right->code == NAME)
    {
        if(rightResult < 0)
        {
            cout<<comparisonOp->right->value<<" not present in any relation"<<endl;
            distinctRight=(double)1.0;
            //return 1.0;
        }
        else
        {
            distinctRight = rightRelation.relAttributes[comparisonOp->right->value].distinctTuples;
        }
    } else 
    {
        distinctRight = -1;
    }
    
//    cout<<endl<<distinctLeft<<" - "<<distinctRight<<endl;
//    if(distinctLeft >= 0 && distinctRight >= 0)
//    {
        if(code == LESS_THAN || code == GREATER_THAN)
        {
            factor = (double)1.0/(double)3.0;
        }
        else if (code == EQUALS)
        {
            if (distinctLeft > distinctRight)
                factor = (double)((double)1.0 / (double)distinctLeft);
            else
                factor = (double)((double)1.0 / (double)distinctRight);
        }
        else
        {
            cout<<"Error"<<endl;
        }
//    }
//    else
//    {
//        factor = 0.0;
//    }
//    cout<<"Factor: "<<factor<<endl;
    return factor;
}


int Statistics::GetRelationForOperand(Operand* operand, char** relName, int numJoin, RelationsStats &relation)
{
    //This function will look at the operand passed to it and get the relation that contains this operand(which is an attribute)
    if(operand == NULL)
        return -1;
    
    if(relName == NULL)
        return -1;
    
    RelationsStats tempRelation;                                    //this is the map of type <char*, RelationStats>
    
    RelationMap::iterator it;
    
    
    for(it = relationMap.begin(); it != relationMap.end(); it++)
    {
        //now check whether this one or the next one contains any of the relations in the relName thingy.
        
        if(it->second.relAttributes.find(operand->value) != it->second.relAttributes.end())
        {
            relation = it->second;
            
            return 0;        
        }
    }

    return -1;    
}


void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    //First check whether there is any relation in here that is not available
    int index = 0;                                                  //Initialize the index to 0, and start searching the relNames array.
    
    RelationsStats relation;
    
    RelationMap::iterator it;
    
    char* newRelNames[100];                                         //This will be the new char array containing the relations.
    
    int newNumToJoin = 0;                                           //This will be the new number of subsets to be considered for join.
    
    while(index < numToJoin)                                        //Check if all the relations exist in the current relationMap, otherwise return.
    {
        it = relationMap.find(relNames[index]);
        
        if (it != relationMap.end())
        {
            relation = it->second;
            
            newRelNames[newNumToJoin++] = relNames[index];
            
            if(!relation.isJoin) {
                
                index++;
                
                continue;
            }
            
            int size = relation.joinRelations.size();
            
            if(size<= numToJoin)
            {
                //the for loop starts from i = index, because all the relations before the index 'index' in the relNames array have been found.
                for(int i=0; i<numToJoin; i++)
                {
                    if((relation.joinRelations.find(relNames[i]) == relation.joinRelations.end()) && (relation.joinRelations[relNames[i]] != relation.joinRelations[relNames[index]]))
                    {
                        cout<<"Cannot be joined"<<endl;
                        
                        return;
                    }
                }
            }
            else
            {
                cout<<"Cannot be joined"<<endl;
            }

        }
        
        index++; 
    }
    
    //Call the estimation here
    double estimatedTuples = Estimate(parseTree, newRelNames, newNumToJoin);
    
    //If it reaches this stage it means join can be done, then at this stage we need to merge the relations together
    index = 1;
    
    string firstRelation = newRelNames[0];
    
    RelationsStats firstRelationObj = relationMap[firstRelation];
    
    relationMap.erase(firstRelation);
    
    RelationsStats tempRelation;
    
    firstRelationObj.isJoin = true;
    
    firstRelationObj.numberOfTuples = estimatedTuples;
    
    while(index < newNumToJoin)
    {
        firstRelationObj.joinRelations[newRelNames[index]] = newRelNames[index];
        
        tempRelation = relationMap[newRelNames[index]];
        
        relationMap.erase(newRelNames[index]);
        
        //merge the attributes from both the relations.
        firstRelationObj.relAttributes.insert(tempRelation.relAttributes.begin(), tempRelation.relAttributes.end());
        
        index++;
    }
    
    relationMap.insert(pair<string, RelationsStats>(firstRelation, firstRelationObj));
}


double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    //Here we are assuming that we have everything correctly.
    
    double tupleProduct = 1.0;
    
    double factor = 1.0;
    
    int index = 0;
    
    while(index < numToJoin)
    {
        if(relationMap.find(relNames[index]) == relationMap.end())
        {
//            cout<<relNames[index]<<" not found!!!"<<endl;
        }
        else
        {
            RelationsStats relation = relationMap[relNames[index]];
            
//            cout<<relNames[index]<<" = "<<(int)relation.numberOfTuples<<endl;
            
            tupleProduct *= (double)relation.numberOfTuples;
        }
        index++;
        //        cout<<"Estimate>>Tuple Product:"<<tupleProduct<<endl;
    }
    
    //cout<<"Product: "<<tupleProduct<<endl;
    
    if (parseTree == NULL)
        return tupleProduct;
    
    factor = And(parseTree, relNames, numToJoin);
    
    //cout<<"Final Factor: "<<factor<<endl;
    
    double returnVal = (double)tupleProduct*factor;
    
//    cout<<"Return Val: "<<(int)returnVal<<endl;
    
    return (returnVal);
}

void Statistics::AddRel(char *relName, int numTuples)
{    
    RelationsStats newRelObject(relName, numTuples);
    
    relationMap[relName] = newRelObject;                    //create a new if one does not already exist, otherwise replace.
}


void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    AttributeStat newAttsObject(attName, numDistincts);
    
    relationMap[relName].relAttributes[attName] = newAttsObject;
}


void Statistics::CopyRel(char *oldName, char *newName)
{          
    relationMap[newName] = relationMap[oldName];
    
    relationMap[newName].relationName = newName;
    
    AttributeMap::iterator it;
    
    AttributeMap newMap;
    
    for(it = relationMap[newName].relAttributes.begin(); it != relationMap[newName].relAttributes.end(); it++)
    {
        string newAttName = newName;
        
        string oldAttName = it->first;
        
        newAttName.append(".");
        
        newAttName.append(oldAttName);
        
        AttributeStat attObject(it->second);
        
        attObject.attName = newAttName;
        
        newMap.insert(pair<string, AttributeStat>(newAttName, attObject));
    }
    
    relationMap[newName].relAttributes = newMap;
    
    //    relationMap[newName].Print();
}
	

void Statistics::Read(char *fromWhere)
{
    
    ifstream inFile(fromWhere);
    if (!inFile)
        return;
    int isJoin;
    int numberOfJoinedRelations;
    double numberOfRelations, numberOfAttributes, numberOfTuples, numberOfDistinctValues;
    //long int numberOfRelations, numberOfAttributes, numberOfTuples, numberOfDistinctValues;
    
    //Clear the current relation map
    relationMap.clear();
    string relationName, attributeName, temp;
    
    //Read in the number of relations
    inFile>>numberOfRelations;
    
    //Add Each Relation
    for (long int i=0;i<numberOfRelations;i++)
    {
        
        //Read the name of the relation
        inFile>>relationName;
        
        //Read the number of tuples in the relation
        inFile>>numberOfTuples;
        
        //Add the relation
        RelationsStats newRelObject(relationName, numberOfTuples);
        relationMap[relationName] = newRelObject;
        
        //Check if it is a join
        inFile>>isJoin;
        relationMap[relationName].isJoin = (isJoin==1);
        if (relationMap[relationName].isJoin)
        {
            inFile>>numberOfJoinedRelations;
            for (int j=0;j<numberOfJoinedRelations;j++)
            {
                inFile>>temp;
                relationMap[relationName].joinRelations[temp]=temp;
            }
        }
        //Read in the number of Attributes in the relation
        inFile>>numberOfAttributes;
        
        //Add Each Attribute:
        for (int j=0;j<numberOfAttributes;j++)
        {
            
            //Read the name of the attribute            
            inFile>>attributeName;
            
            //Read the number of unique values for the attribute   
            inFile>>numberOfDistinctValues;
            
            AttributeStat newAttsObject(attributeName, numberOfDistinctValues);
            
            relationMap[relationName].relAttributes[attributeName] = newAttsObject;
        }
    }
}

void Statistics::Write(char *fromWhere)
{    
    ofstream outFile(fromWhere);
    
    //Write out the number of relations    
    outFile<<(int)relationMap.size()<<endl;

    //Write each relation
    for (RelationMap::iterator relationIterator = relationMap.begin(); relationIterator !=relationMap.end();relationIterator++)
    {
        RelationsStats currentRelation = relationIterator->second;
        //Write the name of the relation
        outFile<<currentRelation.relationName<<endl;
        
        //Write the number of tuples
        outFile<<currentRelation.numberOfTuples<<endl;

        //Write 0 or 1 based on if it's not a join or it is
        //If it is a join write the number of joined relations
        if (currentRelation.isJoin)
        {
            outFile<<(int)1<<endl;
            //Write the number of relations in the joined list
            outFile<<currentRelation.joinRelations.size()<<endl;
            for (map<string, string>::iterator joinIterator=currentRelation.joinRelations.begin(); joinIterator!=currentRelation.joinRelations.end(); joinIterator++)
            {
                //Write the name of each relation
                outFile<<joinIterator->second<<endl;
            }
        }
        else
            outFile<<(int)0<<endl;
        
        //Write the number of attributes
        outFile<<relationIterator->second.relAttributes.size()<<endl;
//        cout<<relationIterator->second.relAttributes.size()<<endl;

        //Write each attribute
        for (AttributeMap::iterator attributeIterator = relationIterator->second.relAttributes.begin(); attributeIterator!=relationIterator->second.relAttributes.end(); attributeIterator++)
        {
            //Write the name of the attribute
            outFile<<attributeIterator->second.attName<<endl;
//            cout<<attributeIterator->second.attName<<endl;

            //Write the number of unique values for the attribute    
            outFile<<attributeIterator->second.distinctTuples<<endl;
        }  
    }
    outFile.close();
}

void Statistics :: Print ()
{
    RelationMap::iterator it;
    for(it = relationMap.begin(); it != relationMap.end(); it++) {
        it->second.Print();
        cout<<"\n";
    }
}


bool Statistics :: isRelationInMap(string relName, RelationsStats &relation)
{
    RelationMap::iterator it;
    
    for(it = relationMap.begin(); it != relationMap.end(); it++)
    {
        if(it->second.isRelationPresent(it->first))
        {
            relation = it->second;
            
            return true;
        }
        
    }
    
    return false;
}





