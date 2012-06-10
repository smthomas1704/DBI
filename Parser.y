 
%{

	#include "ParseTree.h" 
	#include <stdio.h>
	#include <string.h>
	#include <stdlib.h>
	#include <iostream>

	extern "C" int yylex();
	extern "C" int yyparse();
	extern "C" void yyerror(char *s);
  
	// these data structures hold the result of the parsing
	struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
	struct TableList *tables; // the list of tables and aliases in the query
	struct AndList *boolean; // the predicate in the WHERE clause
	struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
	struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
	int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query 
	int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

	char* tableTemp;
	char* fileTemp;
	char* outputType;
	char* fileTypeTemp;
	int queryType;
	struct AttList* attsToSelectTemp;	
	struct AttList* sortedAttsToSelectTemp;


%}

// this stores all of the types returned by production rules
%union {
 	struct FuncOperand *myOperand;
	struct FuncOperator *myOperator; 
	struct TableList *myTables;
	struct ComparisonOp *myComparison;
	struct Operand *myBoolOperand;
	struct OrList *myOrList;
	struct AndList *myAndList;
	struct NameList *myNames;
	char *actualChars;
	char whichOne;

	struct AttList* myAttsNames;
	//char* actualChars;
 	char* myOutputType;
	char* myFileName;
	char* myTableName;
	char* myfileType;
	char* myattName;
	char* myattType;
	int myQueryType;
}

%token <actualChars> Name
%token <actualChars> Float
%token <actualChars> Int
%token <actualChars> String
%token SELECT
%token GROUP 
%token DISTINCT
%token BY
%token FROM
%token WHERE
%token SUM
%token AS
%token AND
%token OR
%token EXIT

%token CREATE
%token TABLE 
%token DROP
//%token AS
%token ON
%token INSERT
%token INTO
%token SET
%token OUTPUT


%type <myOrList> OrList
%type <myAndList> AndList
%type <myOperand> SimpleExp
%type <myOperator> CompoundExp
%type <whichOne> Op 
%type <myComparison> BoolComp
%type <myComparison> Condition
%type <myTables> Tables
%type <myBoolOperand> Literal
%type <myNames> Atts

%type <myOutputType> outputMode
%type <myFileName> fileName
%type <myTableName> tableName
%type <myfileType> fileType
%type <myattName> attName
%type <myattType> attType
%type <myAttsNames> SingleAtt
%type <myAttsNames> Attributes
%type <myAttsNames> SortedSingleAtt
%type <myAttsNames> SortedAttributes


%start SQL


//******************************************************************************
// SECTION 3
//******************************************************************************
/* This is the PRODUCTION RULES section which defines how to "understand" the 
 * input language and what action to take for each "statment"
 */

%%

SQL: EXIT
{
	queryType = 5;
};
SQL: SELECT WhatIWant FROM Tables WHERE AndList
{
	tables = $4;
	boolean = $6;	
	groupingAtts = NULL;
	queryType = 4;
}

| SELECT WhatIWant FROM Tables WHERE AndList GROUP BY Atts
{
	tables = $4;
	boolean = $6;	
	groupingAtts = $9;
	queryType = 4;
};

WhatIWant: Function ',' Atts 
{
	attsToSelect = $3;
	distinctAtts = 0;
}

| Function
{
	attsToSelect = NULL;
}

| Atts 
{
	distinctAtts = 0;
	finalFunction = NULL;
	attsToSelect = $1;
}

| DISTINCT Atts
{
	distinctAtts = 1;
	finalFunction = NULL;
	attsToSelect = $2;
	finalFunction = NULL;
};

Function: SUM '(' CompoundExp ')'
{
	distinctFunc = 0;
	finalFunction = $3;
}

| SUM DISTINCT '(' CompoundExp ')'
{
	distinctFunc = 1;
	finalFunction = $4;
};

Atts: Name
{
	$$ = (struct NameList *) malloc (sizeof (struct NameList));
	$$->name = $1;
	$$->next = NULL;
} 

| Atts ',' Name
{
	$$ = (struct NameList *) malloc (sizeof (struct NameList));
	$$->name = $3;
	$$->next = $1;
}

Tables: Name AS Name 
{
	$$ = (struct TableList *) malloc (sizeof (struct TableList));
	$$->tableName = $1;
	$$->aliasAs = $3;
	$$->next = NULL;
}

| Tables ',' Name AS Name
{
	$$ = (struct TableList *) malloc (sizeof (struct TableList));
	$$->tableName = $3;
	$$->aliasAs = $5;
	$$->next = $1;
}



CompoundExp: SimpleExp Op CompoundExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));
	$$->leftOperator->leftOperator = NULL;
	$$->leftOperator->leftOperand = $1;
	$$->leftOperator->right = NULL;
	$$->leftOperand = NULL;
	$$->right = $3;
	$$->code = $2;	

}

| '(' CompoundExp ')' Op CompoundExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = $2;
	$$->leftOperand = NULL;
	$$->right = $5;
	$$->code = $4;	

}

| '(' CompoundExp ')'
{
	$$ = $2;

}

| SimpleExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = NULL;
	$$->leftOperand = $1;
	$$->right = NULL;	

}

| '-' CompoundExp
{
	$$ = (struct FuncOperator *) malloc (sizeof (struct FuncOperator));	
	$$->leftOperator = $2;
	$$->leftOperand = NULL;
	$$->right = NULL;	
	$$->code = '-';

}
;

Op: '-'
{
	$$ = '-';
}

| '+'
{
	$$ = '+';
}

| '*'
{
	$$ = '*';
}

| '/'
{
	$$ = '/';
}
;

AndList: '(' OrList ')' AND AndList
{
        // here we need to pre-pend the OrList to the AndList
        // first we allocate space for this node
        $$ = (struct AndList *) malloc (sizeof (struct AndList));

        // hang the OrList off of the left
        $$->left = $2;

        // hang the AndList off of the right
        $$->rightAnd = $5;

}

| '(' OrList ')'
{
        // just return the OrList!
        $$ = (struct AndList *) malloc (sizeof (struct AndList));
        $$->left = $2;
        $$->rightAnd = NULL;
}
;

OrList: Condition OR OrList
{
        // here we have to hang the condition off the left of the OrList
        $$ = (struct OrList *) malloc (sizeof (struct OrList));
        $$->left = $1;
        $$->rightOr = $3;
}

| Condition
{
        // nothing to hang off of the right
        $$ = (struct OrList *) malloc (sizeof (struct OrList));
        $$->left = $1;
        $$->rightOr = NULL;
}
;

Condition: Literal BoolComp Literal
{
        // in this case we have a simple literal/variable comparison
        $$ = $2;
        $$->left = $1;
        $$->right = $3;
}
;

BoolComp: '<'
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = LESS_THAN;
}

| '>'
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = GREATER_THAN;
}

| '='
{
        // construct and send up the comparison
        $$ = (struct ComparisonOp *) malloc (sizeof (struct ComparisonOp));
        $$->code = EQUALS;
}
;

Literal : String
{
        // construct and send up the operand containing the string
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = STRING;
        $$->value = $1;
}

| Float
{
        // construct and send up the operand containing the FP number
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = DOUBLE;
        $$->value = $1;
}

| Int
{
        // construct and send up the operand containing the integer
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = INT;
        $$->value = $1;
}

| Name
{
        // construct and send up the operand containing the name
        $$ = (struct Operand *) malloc (sizeof (struct Operand));
        $$->code = NAME;
        $$->value = $1;
}
;


SimpleExp: 

Float
{
        // construct and send up the operand containing the FP number
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = DOUBLE;
        $$->value = $1;
} 

| Int
{
        // construct and send up the operand containing the integer
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = INT;
        $$->value = $1;
} 

| Name
{
        // construct and send up the operand containing the name
        $$ = (struct FuncOperand *) malloc (sizeof (struct FuncOperand));
        $$->code = NAME;
        $$->value = $1;
}
;

SQL: SET OUTPUT outputMode
{
   outputType = $3;
   queryType = 3;
};

outputMode:Name
{	
	$$ = (char*) malloc(strlen($1));
	$$ = $1;
}
| '\'' Name '\''
{
	$$ =(char*) malloc(strlen($2));
	$$ =$2;
};

SQL: INSERT fileName INTO tableName
{
       fileTemp = $2;
       tableTemp = $4;
       queryType = 2;
       
};

SQL: DROP TABLE tableName
{
	tableTemp = $3;
	queryType = 1;
};

SQL: CREATE TABLE tableName AttIWant AS fileType
{
	tableTemp = $3;
	fileTypeTemp = $6;
	queryType = 0;
}
| CREATE TABLE tableName AttIWant AS fileType ON SortedAttIWant
{
	tableTemp = $3;
	fileTypeTemp = $6;
	queryType = 0;
};
SortedAttIWant:  SortedAttributes 
{
	sortedAttsToSelectTemp = $1;
};

SortedAttributes: SortedSingleAtt
{
	$$ = (AttList*) malloc( sizeof( AttList));
	$$->next = NULL;
	$$ = $1;
}

| SortedSingleAtt ',' SortedAttributes
{

	$$ = (AttList*) malloc( sizeof( AttList));
	$$ = $1;
	$$->next = $3;

};

SortedSingleAtt: attName
{
	$$=(AttList*) malloc(sizeof(AttList));
	$$->attributeName=$1;
	
};

//------------------------
AttIWant: '(' Attributes ')'
{
	attsToSelectTemp = $2;
};

Attributes: SingleAtt
{
	$$ = (AttList*) malloc( sizeof( AttList));
	$$->next = NULL;
	$$ = $1;
}

| SingleAtt ',' Attributes
{

	$$ = (AttList*) malloc( sizeof( AttList));
	$$ = $1;
	$$->next = $3;

};

SingleAtt: attName attType
{
	$$=(AttList*) malloc(sizeof(AttList));
	$$->attributeName=$1;
	$$->attributeType=$2;
};
attName: Name
{
	$$ = (char*) malloc(strlen($1));
	$$=$1;
};
attType: Name
{
	$$=(char*) malloc(strlen($1));
	$$=$1;
};
fileType: Name
{
	$$=$1;
};



fileName: Name
{

	$$ = (char*) malloc(strlen($1));
	$$ = $1;
}
| '\'' Name '\''
{
	$$ = (char*) malloc(strlen($2));
	$$ = $2;
};

tableName: Name
{

	$$ = (char*) malloc(strlen($1));
	$$ = $1;
};


%%

