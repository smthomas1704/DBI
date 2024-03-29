CC = g++ -O2 -Wno-deprecated 

tag = -i

ifdef linux
tag = -n
endif

a4-2.out: Record.o Comparison.o ComparisonEngine.o Function.o Schema.o File.o DBFile.o Statistics.o RelOp.o QTree.o BigQ.o Pipe.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o
	$(CC) -o a4-2.out Record.o Comparison.o ComparisonEngine.o Function.o Schema.o File.o DBFile.o Statistics.o RelOp.o QTree.o BigQ.o Pipe.o y.tab.o yyfunc.tab.o lex.yy.o lex.yyfunc.o main.o -lfl

QTree.o: QTree.cc
	$(CC) -g -c QTree.cc

main.o: main.cc
	$(CC) -g -c main.cc

Statistics.o: Statistics.cc
	$(CC) -g -c Statistics.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Function.o: Function.cc
	$(CC) -g -c Function.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc

RelOp.o: RelOp.cc
	$(CC) -g -c RelOp.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	#sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.*
	rm -f yyfunc.tab.*
	rm -f lex.yy.*
	rm -f lex.yyfunc*
