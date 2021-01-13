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

/* the Tx mgr functions are implemented here */

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <sys/sem.h>
#include <fstream>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"

#define TEAM_NO 19

/* Prints the header description */

void zgt_tm::openlog(string lfile)
{
    #ifdef TM_DEGUG
        printf("entering openlog\n");fflush(stdout);
        fflush(stdout);
    #endif
    
    printf("---------------------------------------------------------------\n");
    printf("TxId\tTxtype\tOperation\tObId:Obvalue:optime\tLockType\tStatus\t\tTxStatus\n");
    fflush(stdout);
    
    #ifdef TM_DEBUG
        printf("leaving openlog\n");fflush(stdout);
        fflush(stdout);
    #endif
}

/* Creates a thread and calls the constructor of transaction to */
/* create the object and intialize the other members of zgt_tx in */
/* begintx(void *thdarg). Passes the thread arguments in a structure. */

int zgt_tm::BeginTx(long tid, int thrNum, char type)
{
    #ifdef TM_DEBUG
        printf("\ncreating BeginTx thread for Tx: %d\n", tid);
        fflush(stdout);
    #endif
    
    struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
    pthread_t thread1;
    nodeinfo->tid = tid;
    nodeinfo->Txtype = type;
    nodeinfo->obno = -1;
    nodeinfo->count= SEQNUM[tid]=0;
    
    int status;
    status = pthread_create(&threadid[thrNum], NULL, begintx,(void*)nodeinfo);	// Fork a thr with tid
    if (status)
    {
        printf("ERROR: return code from pthread_create() is:%d\n", status);
        exit(-1);
    }
    
    #ifdef TM_DEBUG
        printf("\nfinished creating BeginTx thread for Tx: %d\n", tid);
        fflush(stdout);
    #endif
    
    return(0);
 }     

/* Sets the TxMgr semaphore first. Creates a thread and first checks */
/* whether this thread can proceed based on the condition variable for that thread. */
/* This is to prevent 2 operations of the same Tx follow one another. */
/* Then gets the lock and performs the read opeartion. */
/* Calls the read function in transaction. Read operation is just printing the value of the item. */

int zgt_tm::TxRead(long tid, long obno, int thrNum)
{
    #ifdef TM_DEBUG
        printf("\ncreating TxRead thread for Tx: %d\n", tid);fflush(stdout);
        fflush(stdout);
    #endif
    
    struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
    nodeinfo->tid = tid;
    nodeinfo->obno = obno;
    nodeinfo->Txtype = ' ';
    nodeinfo->count = --SEQNUM[tid];
    
    pthread_t thread1;
    int status = pthread_create(&threadid[thrNum],NULL,readtx,(void*)nodeinfo);
    
    if (status)
    {
        printf("ERROR: return code from pthread_create() is:%d\n", status);
        exit(-1);
    }

    #ifdef TM_DEBUG
        printf("\nexiting TxRead thread create for Tx: %d\n", tid);
        fflush(stdout);
    #endif
    
    return(0);
}

/* Similar to TxRead. Write operation is to increement the value by 1. */

int zgt_tm::TxWrite(long tid, long obno, int thrNum)
{
    #ifdef TM_DEBUG
        printf("\nentering TxWrite\n");fflush(stdout);
        fflush(stdout);
    #endif
    
    struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
    nodeinfo->tid = tid;
    nodeinfo->obno = obno;
    nodeinfo->Txtype = ' ';
    nodeinfo->count = --SEQNUM[tid];

    pthread_t thread1;
    int status = pthread_create(&threadid[thrNum],NULL,writetx,(void*)nodeinfo);
    
    if (status)
    {
        printf("ERROR: return code from pthread_create() is:%d\n", status);
        exit(-1);
    }

    #ifdef TM_DEBUG
        printf("\nleaving TxWrite\n");
        fflush(stdout);
    #endif

    return(0);
}

/* Similar to TxRead. Commit operation is to free all locks/objects */
/* held by the commiting Tx and remove that Tx from the TM table */

int zgt_tm::CommitTx(long tid, int thrNum)
{
    #ifdef TM_DEBUG
        printf("\nentering TxCommit\n");fflush(stdout);
        fflush(stdout);
    #endif

    struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
    nodeinfo->tid = tid;
    nodeinfo->count = --SEQNUM[tid];

    pthread_t thread1;
    int status = pthread_create(&threadid[thrNum],NULL,committx,(void*)nodeinfo);
    
    if (status)
    {
        printf("ERROR: return code from pthread_create() is:%d\n", status);
        exit(-1);
    }

    #ifdef TM_DEBUG
        printf("\nleaving TxCommit\n");
        fflush(stdout);
    #endif
    
    return(0);
}
 
/* Similar to CommitTx */

int zgt_tm::AbortTx(long tid, int thrNum)
{
    #ifdef TM_DEBUG
        printf("\nentering TxAbort\n");fflush(stdout);
        fflush(stdout);
    #endif

    struct param *nodeinfo = (struct param*)malloc(sizeof(struct param));
    nodeinfo->tid = tid;
    nodeinfo->count = --SEQNUM[tid];

    pthread_t thread1;
    int status = pthread_create(&threadid[thrNum],NULL,aborttx,(void*)nodeinfo);
    
    if (status)
    {
        printf("ERROR: return code from pthread_create() is:%d\n", status);
        exit(-1);
    }

    #ifdef TM_DEBUG
        printf("\nleaving TxAbort\n");
        fflush(stdout);
    #endif
    
    return(0);
}

/* Called when end all is read from input */

int zgt_tm::endTm(int thrNum)
{
    #ifdef TM_DEBUG
        printf("\nEntering End of schedule thread with thrNum: %d\n", thrNum);
        fflush(stdout);
    #endif
//    printf("Wait for threads and cleanup\n");
    
    int rc=0;
    int i;

    for (i=0; i < thrNum; i++)
    {
        rc = pthread_join(threadid[i], NULL);
//        printf("Thread %d completed with ret value: %d\n", i, rc);
//        fflush(stdout);
    }

//    printf("ALL threads finished their work\n");
//    fflush(stdout);
//    printf("Releasing mutexes and condpool\n");
//    fflush(stdout);

    // Release condpool and mutexpool

    for(i=1;i<MAX_TRANSACTIONS+1;++i)
    {
        pthread_mutex_destroy(&mutexpool[i]);
        pthread_cond_destroy(&condpool[i]);
    }

//    printf("Releasing all semaphores\n");
//    fflush(stdout);
    zgt_sem_release(ZGT_Semid);
//    printf("endTm completed\n");
//    fflush(stdout);

    #ifdef TM_DEBUG
        printf("\nFinished end of schedule thread: endTm\n");
        fflush(stdout);
    #endif

    fclose(this->logfile);

    return(0);
}

/* Currently not used */

int zgt_tm::ddlockDet()
{
    #ifdef TM_DEBUG
        printf("\nentering ddlockDet\n");
        fflush(stdout);
    #endif

//    pthread_t thread1;
//    int status=pthread_create(&thread1, NULL,ddlockdet,(void*)NULL); // Fork a thread and pass tid
//
//    if (status)
//    {
//        printf("ERROR: return code from pthread_create() is:%d\n", status);
//        exit(-1);
//    }
    
    #ifdef TM_DEBUG
        printf("\nleaving ddlockDet\n");
        fflush(stdout);
    #endif
    
    return(0);
}

/* Currently not used */

int zgt_tm::chooseVictim()
{
    #ifdef TM_DEBUG
        printf("\nentering chooseVictim\n");
        fflush(stdout);
    #endif

//    pthread_t thread1;
//    int status =pthread_create(&thread1, NULL,choosevictim,(void*)NULL); // Fork a thread and pass tid
//
//    if (status)
//    {
//        printf("ERROR: return code from pthread_create() is:%d\n", status);
//        exit(-1);
//    }
    
    #ifdef TM_DEBUG
        printf("\nleaving chooseVictim\n");
        fflush(stdout);
    #endif
    
    return(0);
}

zgt_tm::zgt_tm()
{
    #ifdef TM_DEBUG
        printf("\nInitializing the TM\n");
        fflush(stdout);
    #endif
    
    int i,init;

    lastr = NULL;
    
    // Initialize objarray; each element points to a different item
    for(i=0;i<MAX_ITEMS;++i)
        objarray[i] = new item(0);  //all init'd to zero

    // Initialize optime for the thread to sleep;
    // get a int from random function to sleep
    int seed = 7919; //prime num
    srand(seed); // initialize random number generator
    int M = 1000ll; // Multiplier
    
    for(i=1; i<MAX_TRANSACTIONS+1; ++i)
    {
        double r = ( (double)rand() / (double)(RAND_MAX+1.0) ); //RAND_MAX is defined in stdlib
        double x = (r * M * TEAM_NO);
        int y = (int) x;
        optime[i]= abs(y);
        #ifdef TM_DEBUG
            printf("\nValues of i, r, x, and y are: %d, %f, %f, %d\n", i, r, x, y);
            fflush(stdout);
        #endif
    }


    // Initialize condpool and mutexpool
    for(i=1;i<MAX_TRANSACTIONS+1;++i)
    {
        pthread_mutex_init(&mutexpool[i],NULL);
        pthread_cond_init(&condpool[i],NULL);
        condset[i] = 0;
        SEQNUM[i] = 0;
    }
    
    ZGT_Nsema = MAX_TRANSACTIONS+1 ; //setting the no of semaphore

    ZGT_Key_sem = TEAM_NO; //setting the key_t data to const

    // Semget() gets a array of semaphore for a particular key.Here
    // creating a semaphore with  key 1

    if ((sem= zgt_init_sema(IPC_CREAT))<0)
    {
        cout<< "Error creating semaphores \n";
        exit(1);
    }

    ZGT_Semid = sem;

    // Intialising the semaphore value with 0 to 1 and the rest to 0
    zgt_init_sema_0(ZGT_Semid);
    zgt_init_sema_rest(ZGT_Semid);

    #ifdef TM_DEBUG
        printf("\nleaving TM initialization\n");
        fflush(stdout);
    #endif
};

 
 










