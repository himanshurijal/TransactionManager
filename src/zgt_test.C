//  Copyright [2021] [Himanshu Rijal]
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

/* Main program that processes input and invokes the Tx mgr methods. */
/* The main data structures are initialized here */

#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <ctype.h>
#include <sys/signal.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <string>
#include <fstream>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_global.h"
#include "zgt_extern.h"

#define MAX_TOKENS 10
#define MAX_INPUT_STRING 100

void Tokenize(const string& str,string tokens[MAX_TOKENS],
                  const string& delimiters = " ")
{
    // Skip delimiters at beginning.
    string::size_type lastPos = str.find_first_not_of(delimiters, 0);
    // Find first "non-delimiter".
    string::size_type pos = str.find_first_of(delimiters, lastPos);
    int i = 0;
    
    while (string::npos != pos || string::npos != lastPos)
    {
        // Found a token, add it to the array.
        tokens[i] = str.substr(lastPos,pos-lastPos);
        i++;

        // Skip delimiters.  Note the "not_of"
        lastPos = str.find_first_not_of(delimiters, pos);

        // Find next "non-delimiter"
        pos = str.find_first_of(delimiters, lastPos);
    }
}


int string2int(char *s,string str)
{
    int i =0;
    s = (char *) malloc (sizeof(char) * 12);
    
    while (str[i]!='\0')
    {
        s[i] = str[i];
        i++;
    }
    
    int k = atoi(s);
    
    return(k);
}

int main(int argn, char **argv)
{
    long sgno=1, obno,tid;
    char lockmode,Txtype, temp[2]; // To convert str to char
    char *infilename;
    zgt_tx *t;
    string delimiters = " ";
    char str[MAX_INPUT_STRING];
    string s;
    int op;
    char *c;
    int thrNum =0;

    if (argn < 2)
    {
        printf("USAGE:\n");
        printf("\tzgt_test <input file name WITH extension>\n" ) ;
        exit(1);
    }

    infilename = argv[1];
    ifstream inFile(infilename,ios::in);

 
    if (! inFile.is_open())
    {
        cout << "\nError opening file: " << infilename << ".txt\n";
        exit (1);
    }

    // If invoked correctly, create one transaction manager object
    // also the hash table used as lock table

    ZGT_Sh = new zgt_tm();
    ZGT_Ht = new zgt_ht(ZGT_DEFAULT_HASH_TABLE_SIZE);

    inFile.getline (str,MAX_INPUT_STRING);
    while (!inFile.eof())
    {   
        s = str;
        // cout << s << "\n";
        string tokens[MAX_TOKENS];
        Tokenize(s, tokens);

       
        if (tokens[0] == "//" )
        {
            cout << s << "\n";
        }
        else if(tokens[0] == "Log" || tokens[0] == "log")
        {
//            cout << "Log file name:" << tokens[1] << "\n\n";
            ZGT_Sh->openlog(tokens[1]);
        }
        else if(tokens[0] == "BeginTx" || tokens[0] == "begintx")
        {
            tid = string2int(c,tokens[1]);
            
            // Added following two statements to get Txtype.
            // Passing the Txtype to get the transaction type.
            strcpy(temp,tokens[2].c_str());
            Txtype = temp[0];

            if ((op=ZGT_Sh->BeginTx(tid, thrNum++,Txtype))<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            //        {  //error code }
        }
        else if(tokens[0] == "Read" || tokens[0] == "read")
        {
            int k = string2int(c,tokens[1]);
            tid = k;

            k = string2int(c,tokens[2]);
            obno = k;

            if((op= ZGT_Sh->TxRead(tid,obno, thrNum++))<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            //  { //error code }
        }
       else if(tokens[0] == "Write" ||  tokens[0] == "write")
       {
            int k = string2int(c,tokens[1]);
            tid = k;

            k = string2int(c,tokens[2]);
            obno = k;

            if((op= ZGT_Sh->TxWrite(tid,obno, thrNum++))<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            // { //error code}
        }
       else if(tokens[0] == "Abort" || tokens[0] == "abort")
       {
            int k = string2int(c,tokens[1]);
            tid = k;

            if((op= ZGT_Sh->AbortTx(tid, thrNum++))<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            // { //error code}
       }
       else if(tokens[0] == "Commit" || tokens[0] == "commit")
       {
            int k = string2int(c,tokens[1]);
            tid = k;
           
            if((op= ZGT_Sh->CommitTx(tid, thrNum++))<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            // { //error code}
       }
       else if(tokens[0] == "Detect" || tokens[0] == "detect")
       {
            printf("Detect Cycles :\n\n");
           
            if((op= ZGT_Sh->ddlockDet())<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            // { //error code}
       }
       else if(tokens[0] == "choose" || tokens[0] == "Choose")
       {
            printf("Detect Cycles AND choose a victim:\n\n");
           
            if((op= ZGT_Sh->chooseVictim())<0)
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
            // { //error code}
       }
       else if(tokens[0] == "end" || tokens[0] == "End")
       {
            fflush(stdout);
           
            if((op= ZGT_Sh->endTm(thrNum))<0) // Removed ++ sharma oct 2020
                cout << "\nerro from:" << tokens[0] <<" for TID:" << tid << "\n";
           
            fflush(stdout);
            inFile.close();
            pthread_exit(NULL);
        }
       else
       {
            cout << "\ninput error:" << tokens[0] << "," << tokens[1] << "," << tokens[2] << "," << tokens[3] << "\n";
            cout << endl;
            fflush(stdout);
            inFile.close();
            pthread_exit(NULL);
       }
    
       inFile.getline (str,MAX_INPUT_STRING);
    }
    
    cout << endl;
    inFile.close();
    pthread_exit(NULL);
}






