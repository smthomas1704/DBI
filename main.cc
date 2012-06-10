#include <iostream>
#include <fstream>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <sstream>
#include <vector>
#include "ParseTree.h"
#include "DBFile.h"
#include "QTree.h"


static char *dbfile_dir = "./DBFiles/"; // dir where binary heap files should be stored
//static char * tpch_dir = "/cise/tmp/dbi_sp11/DATA/1G/"; // dir where dbgen tpch files (extension *.tbl) can be found
static char *catalog_path = "catalog"; // full path of the catalog file

using namespace std;

extern "C" {
    int yyparse(void); // defined in y.tab.c
}

extern char* outputType;
extern char* fileTemp;
extern char* tableTemp;
extern char* fileTypeTemp;
extern struct AttList* attsToSelectTemp;
extern struct AttList* sortedAttsToSelectTemp;
extern struct TableList* tables;
extern struct AndList* andList;
extern struct NameList* names;

extern int queryType;

void execute_create()
{
//    //Create the Dbfile
//    DBFile dbfile;
//    char rpath[100]; //path of the bin file
//    sprintf (rpath, "%s%s.bin",dbfile_dir, tableTemp);
//    cout <<tableTemp<<".bin"<<" created at " << rpath << "\n" <<"Catalog Updated"<<"\n\n";
//    dbfile.Create (rpath, heap, NULL);
//    dbfile.Close ();
//
//    //Make entry in catalog file
//      const char* catalog = "catalog";
//      ofstream catfile;
//      catfile.open(catalog,ios::app);
//      catfile<<"BEGIN\n";
//      catfile<<tableTemp<<"\n";
//      catfile<<tableTemp<<".tbl\n";
//      while(attsToSelectTemp !=NULL)
//      {
//          catfile<<attsToSelectTemp->attributeName<<" ";
//          if(strcmp(attsToSelectTemp->attributeType,"DOUBLE") == 0)
//              catfile<<"Double\n";
//          else if(strcmp(attsToSelectTemp->attributeType,"INT") == 0)
//              catfile<<"Int\n";
//          else if(strcmp(attsToSelectTemp->attributeType,"STRING") == 0)
//              catfile<<"String\n";
//          attsToSelectTemp = attsToSelectTemp->next;
//      }
//      catfile<<"END\n\n";
//      catfile.close();
}

void execute_drop()
{
//    //Delete the dbfile
//    char rpath[100];
//    char metapath[100];
//    sprintf(rpath,"%s%s.bin", dbfile_dir, tableTemp);
//    remove(rpath);
//    sprintf(metapath,"%s.meta", rpath); remove(metapath);
//    cout<<tableTemp<<".bin"<<" dropped from "<<dbfile_dir<<endl;
//
//
//    //Delete the Entry in Catalog.
//    string line ,temp;
//    const char* catalog = "catalog";
//    const char* src;
//    ifstream catFile(catalog);
//    ofstream tempcatFile("tempcatFile");
//
//    while(getline(catFile,line))
//    {
//	getline(catFile,line);
//        if(line=="BEGIN")
//            temp =line;
//        getline(catFile,line);
//        if(strcmp(tableTemp,line.c_str())==0)
//        {
//            do
//            {
//                getline(catFile,line);
//            }
//            while(line!="END");
//            break;
//        }
//        else
//        {
//	    tempcatFile<<"\n"<<temp<<"\n";
//            do
//            {
//                getline(catFile,line);
//                tempcatFile<<line<<"\n";
//            }
//            while(line!="END");
//        }
//    }
//
//    //for remaining relations in schema file
//    while(getline(catFile,line))
//    {
//        tempcatFile<<line<<"\n";
//    }
//    catFile.close();
//    tempcatFile.close();
//    remove(catalog);
//    rename("tempcatFile",catalog);
//    tempcatFile<<"\n";
//    cout<<"Catalog Updated\n\n";
   
}

void execute_insert()
{
//   
//    DBFile dbfile;
//
//    //Set up the bin path
//    char bin_path[100];
//    sprintf (bin_path, "%s%s.bin", dbfile_dir, tableTemp);
//
//    //Set up the tpch_path
//    char tpch_path[100];
//    sprintf (tpch_path, "%s%s", tpch_dir, fileTemp);
//
//    dbfile.Open(bin_path);
//    Schema sch(catalog_path,tableTemp);
//    dbfile.Load (sch, tpch_path);
//
//    dbfile.Close ();

}

void set_output_type()
{
//    if (strcmp(outputType, "NONE") == 0)
//    {
//        printTree = 1;
//    }
//    else
//    {
//        if (strcmp(outputType, "STDOUT") == 0)
//        {
//            WriteOnFile = 0;
//            printTree = 0;
//        }
//        else
//        {
//            WriteOnFile = 1;
//            printTree = 0;
//        }
//    }
}


int main() {

    cout<<"\n[Enter Valid SQL queries and press Ctrl+D to Execute. Execute EXIT to Stop]\n\n";

    
    /** For testing purposes just hardcode the TableList, OrList etc
     ** then from there call the BuildTree and other functions.
     **/
    
    while (queryType != 5)
    {
        if (yyparse() != 0)
        {
            fprintf(stdout, "Not a Valid Query");
        } 
        
        else
        {
            cout << endl;

            switch (queryType)
            {

                case 0:
                {
                    execute_create();
                     break;
                }
                case 1:
                {
                    execute_drop();
                     break;
                }
                case 2:
                {
                    cout<<"\nExecuting INSERT Query : \n";
                    execute_insert();
                    cout<<"\nINSERT Query Finished\n";
                    break;
                }
                case 3:
                {
                    set_output_type();
                    break;
                }
                case 4:
                {
                    cout<<"\nExecuting SELECT Query : \n";
                    
                    QTree qtree;
                    qtree.Initialize();
                    
                    cout << "\nSELECT Query Finished \n";
                    
                    break;
                }
                case 5:
                    exit(1);
                default:
                    cout << "Check you Query\n";
                    break;
            }
        }
    }
}


