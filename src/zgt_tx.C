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

/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

extern void *start_operation(long, long);  // Start an op with mutex lock and cond wait
extern void *finish_operation(long);       // Finish an op with mutex unlock and con signal
extern void *do_commit_abort(long, char);  // Commit/abort based on char value
extern void *process_read_write(long, long, int, char);

extern zgt_tm *ZGT_Sh;  // Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus,char type, pthread_t thrid)
{
    this->lockmode = (char)' ';
    this->Txtype = type; // R = read only, W=Read/Write
    this->sgno =1;
    this->tid = tid;
    this->obno = -1; // Set it to a invalid value
    this->status = Txstatus;
    this->pid = thrid;
    this->head = NULL;
    this->nextr = NULL;
    this->semno = -1; // Init to  an invalid sem value
}

/* Method used to obtain reference to a transaction node */
/* Inputs the transaction id. Makes a linear scan over the */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL */

zgt_tx* get_tx(long tid1)
{
    zgt_tx *txptr, *lastr1;

    if(ZGT_Sh->lastr != NULL) // If the list is not empty
    {
        lastr1 = ZGT_Sh->lastr; // Initialize lastr1 to first node's ptr
        
        for (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
        {
            if (txptr->tid == tid1) // if required id is found
               return txptr;
        }
        
        return (NULL); // If not found in list return NULL
    }

    return(NULL); // If list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list. New nodes are added to */
/* the start of the transaction list. */

void *begintx(void *arg)
{
    // Intialise a transaction object. Make sure it is
    // done after acquiring the semaphore for the tm and making sure that
    // the operation can proceed using the condition variable. When creating
    // the tx object, set the tx to TR_ACTIVE and obno to -1; there is no
    // semno as yet as none is waiting on this tx.

    struct param *node = (struct param*)arg; // get tid and count
    
    start_operation(node->tid, node->count);
    
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self()); // Create new tx node

    zgt_p(0); // Lock Tx manager; Add node to transaction list
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;
    zgt_v(0); // Release tx manager

    printf("T%d\t%c \tBeginTx\n", node->tid, node->Txtype); // Write log record and close
    fflush(stdout);
    
    finish_operation(node->tid);
    
    pthread_exit(NULL);
}

/* Method to handle Readtx action in test file.   */
/* Inputs a pointer to structure that contains    */
/* tx id and object no. to read. Reads the object */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg)
{
    struct param *node = (struct param*)arg; // Get tid, objno, and count

    start_operation(node->tid, node->count);

    zgt_tx *txPtr = get_tx(node->tid);

    zgt_p(0); // Lock the transaction manager table
    zgt_hlink *objNodePtr = ZGT_Ht->find(1, node->obno); // Find if the object exists in the lock table.
    zgt_v(0); // Unlock the transaction manager table.

    if(objNodePtr == NULL)
    {
        // If the object doesn't exist in the lock table
        // then add the object to the table, and grant the lock
        // (to the object) to the requesting transaction.
        
        zgt_p(0); // Lock the transaction manager table
        ZGT_Ht->add(txPtr,1,node->obno,'S'); // Add the object to the lock table.
        zgt_v(0); // Unlock the transaction manager table.
        
        txPtr->perform_readWrite(node->tid, node->obno, 'S');
    }
    else
    {
        // If the object exists in lock table it means
        // that a transaction is using the object.
        
        if(objNodePtr->tid == node->tid)
        {
            // If object is being used by the same transaction
            // that is requesting the object, grant the lock.
            
            txPtr->perform_readWrite(node->tid, node->obno, 'S');
        }
        else
        {
            // If object is being used by a different
            // transaction than the one requesting the
            // lock.
            
            char requestingTxType = txPtr->Txtype;

            char lockingTxType = get_tx(objNodePtr->tid)->Txtype;

            if(requestingTxType == 'R' && lockingTxType == 'R'
                            && zgt_nwait(objNodePtr->tid) <= 0)
            {
                // If both the requesting and locking
                // transactions are Read-Only(R) and nobody
                // is waiting on the semaphore of the locking
                // transaction grant the lock.

                txPtr->perform_readWrite(node->tid, node->obno, 'S');
            }
            else // Else put on wait queue
            {
                txPtr->status = TR_WAIT; // Change the status of the requesting
                                         // transaction to waiting.
                txPtr->obno = node->obno;
                txPtr->lockmode = 'S';

                txPtr->setTx_semno(objNodePtr->tid, objNodePtr->tid); // Put the requesting transaction
                                                                      // into the wait queue.

                zgt_p(objNodePtr->tid); // Requesting transaction gets the lock
                                        // on the semaphore.

                txPtr->status = TR_ACTIVE; // Change the status of the requesting
                                           // transaction to active.
                txPtr->obno = -1;
                txPtr->lockmode = 'S';

                txPtr->perform_readWrite(node->tid, node->obno, 'S');

                zgt_v(objNodePtr->tid); // Release the lock on the semaphore.
            }
        }
    }

    finish_operation(node->tid);

    pthread_exit(NULL);
}

/* Operation is similar to readtx`*/

void *writetx(void *arg)
{
    struct param *node = (struct param*)arg; // Get tid and objno and count
    
    start_operation(node->tid, node->count);

    zgt_tx *txPtr = get_tx(node->tid);

    zgt_p(0); // Lock the transaction manager table
    zgt_hlink *objNodePtr = ZGT_Ht->find(1, node->obno); // Find if the object exists in the lock table.
    zgt_v(0); // Lock the transaction manager table.

    if(objNodePtr == NULL)
    {
        // If the object doesn't exist in the lock table
        // then add the object to the table, and grant the lock
        // (to the object) to the requesting transaction.
        
        zgt_p(0); // Lock the transaction manager table
        ZGT_Ht->add(txPtr,1,node->obno,'X'); // Add the object to the lock table.
        zgt_v(0); // Unlock the transaction manager table.

        txPtr->perform_readWrite(node->tid, node->obno, 'X');
    }
    else
    {
        // If the object exists in lock table it means
        // that a transaction is using the object.

        if(objNodePtr->tid == node->tid) // If object is being used by the same transaction
                                       // that is requesting the object, grant the lock.
        {
            txPtr->perform_readWrite(node->tid, node->obno, 'X');
        }
        else
        {
            // If object is being used by a different
            // transaction than the one requesting the
            // lock then put object on wait queue.
            
            txPtr->status = TR_WAIT; // Change the status of the requesting
                                     // transaction to waiting.
            txPtr->obno = node->obno;
            txPtr->lockmode = 'X';

            txPtr->setTx_semno(objNodePtr->tid, objNodePtr->tid); // Put the requesting transaction
                                                            // into the wait queue.

            zgt_p(objNodePtr->tid); // Requesting transaction gets the lock
                              // on the semaphore.

            txPtr->status = TR_ACTIVE; // Change the status of the requesting
                                 // transaction to active.
            txPtr->obno = -1;
            txPtr->lockmode = 'X';

            txPtr->perform_readWrite(node->tid, node->obno, 'X');

            zgt_v(objNodePtr->tid); // Release the lock on the semaphore.
        }
    }

    finish_operation(node->tid);

    pthread_exit(NULL);
}

void *aborttx(void *arg)
{
    struct param *node = (struct param*)arg; // Get tid and count

    start_operation(node->tid, node->count);

    zgt_tx *txPtr = get_tx(node->tid);

    printf("T%d\t\tAbortTx\t", node->tid);    // Write log record and close
    fflush(stdout);

    if(txPtr != NULL) // If the transaction exists.
    {
        txPtr->free_locks(); // Free the locks/objects before committing the transaction.
        txPtr->remove_tx(); // Once the locks/objects are freed simply remove the transaction.
    }
    else
    {
        printf("Trying to Remove a Tx:%d that does not exist\n", node->tid);
        fflush(stdout);
    }

    finish_operation(node->tid);
    
    pthread_exit(NULL);
}

/* Method to commit a transaction. The commit operation frees */
/* all the locks/objects held by the committing Tx and */
/* releases all Txs waiting on the committing Tx. */
/* Finally, the commiting Tx object is removed from the TM table */

void *committx(void *arg)
{
    struct param *node = (struct param*)arg;// get tid and count

    start_operation(node->tid, node->count);

    zgt_tx *txPtr = get_tx(node->tid);

    printf("T%d\t\tCommitTx\t", node->tid);    // Write log record and close
    fflush(stdout);

    if(txPtr != NULL) // If the transaction exists.
    {
      txPtr->free_locks(); // Free the locks/objects before committing the transaction.
      txPtr->remove_tx(); // Once the locks/objects are freed simply remove the transaction.
    }
    else
    {
      printf("Trying to Remove a Tx:%d that does not exist\n", node->tid);
      fflush(stdout);
    }

    finish_operation(node->tid);
    pthread_exit(NULL);
}

/* Called from commit/abort with appropriate parameter to do the actual */
/* operation. Make sure you give error messages if you are trying to */
/* commit/abort a non-existant tx */

void *do_commit_abort(long t, char status)
{
    // Currently ot used
}

/* Remove the transaction from the TM table */

int zgt_tx::remove_tx ()
{
    zgt_tx *txptr, *lastr1;
    
    lastr1 = ZGT_Sh->lastr;
    
    for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr) // Scan through list
    {
        if (txptr->tid == this->tid)
        {
            // If correct node is found
            
            lastr1->nextr = txptr->nextr; // Update nextr value; done
                                          // delete this;
            return(0);
        }
        else
        {
            lastr1 = txptr->nextr; // Else update prev value
        }
    }
    
    printf("Trying to Remove a Tx:%d that does not exist\n", this->tid);
    fflush(stdout);
    
    return(-1);
}

/* This method sets lock on objno1 with lockmode1 for a tx */

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1)
{
  //if the thread has to wait, block the thread on a semaphore from the
  //sempool in the transaction manager. Set the appropriate parameters in the
  //transaction list if waiting.
  //if successful  return(0); else -1
  
}

/* This part frees all locks owned by the transaction */
/* that is, remove the objects from the hash table */
/* and release all Tx's waiting on this Tx */

int zgt_tx::free_locks()
{
    zgt_hlink* temp = head; // First obj of tx

    for(temp;temp != NULL;temp = temp->nextp) // Scan Tx obj list
    {
        printf("%d : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
        fflush(stdout);

        if (ZGT_Ht->remove(this,1,(long)temp->obno) == 1)
        {
            printf(":::ERROR:node with tid:%d and onjno:%d was not found for deleting", this->tid, temp->obno);		// Release from hash table
            fflush(stdout);
        }
        else
        {
            #ifdef TX_DEBUG
                printf("\n:::Hash node with Tid:%d, obno:%d lockmode:%c removed\n",
                                temp->tid, temp->obno, temp->lockmode);
                fflush(stdout);
            #endif
        }
    }

    printf("\n");
    fflush(stdout);

    return(0);
}		

/* Used to Commit */
/* Removes the transaction and frees all associated objects. */
/* For the time being this can be used for commit of the transaction. */

int zgt_tx::end_tx()
{
    zgt_tx *linktx, *prevp;

    linktx = prevp = ZGT_Sh->lastr;

    while (linktx)
    {
        if (linktx->tid  == this->tid)
            break;
        prevp  = linktx;
        linktx = linktx->nextr;
    }
    
    if (linktx == NULL)
    {
        printf("\ncannot remove a Tx node; error\n");
        fflush(stdout);
        return (1);
    }
    if (linktx == ZGT_Sh->lastr)
    {
        ZGT_Sh->lastr = linktx->nextr;
    }
    else
    {
        prevp = ZGT_Sh->lastr;
        while (prevp->nextr != linktx) prevp = prevp->nextr;
        prevp->nextr = linktx->nextr;
    }
}

/* Routine to print the tx list */
/* TX_DEBUG should be defined in the Makefile to print */

void zgt_tx::print_tm()
{
    zgt_tx *txptr;

    #ifdef TX_DEBUG
        printf("printing the tx  list \n");
        printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
        fflush(stdout);
    #endif

    txptr=ZGT_Sh->lastr;

    while (txptr != NULL)
    {
        #ifdef TX_DEBUG
            printf("%d\t%c\t%d\t%d\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
            fflush(stdout);
        #endif
        
        txptr = txptr->nextr;
    }

    fflush(stdout);
}

/* Routine for printing for debugging */

void zgt_tx::print_wait()
{
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

/* Routine for printing for debugging */

void zgt_tx::print_lock()
{
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

/* Routine to perform the acutual read/write operation */
/* based on the lockmode */

void zgt_tx::perform_readWrite(long tid,long obno, char lockmode)
{
    int objValue = ZGT_Sh->objarray[obno]->value;
      
    if(lockmode == 'S')
    {
        printf("T%d\t\tReadTx\t\t%d:%d:%d\t\tReadLock\tGranted\t\t%c\n",
                          tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
        fflush(stdout);
    }
    else if(lockmode == 'X')
    {
        ZGT_Sh->objarray[obno]->value = objValue + 1; // Increase object value by 1.
        
        printf("T%d\t\tWriteTx\t\t%d:%d:%d\t\tWriteLock\tGranted\t\t%c\n",
                          tid, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
        fflush(stdout);
    }
}

/* Routine that sets the semno in the Tx when another tx waits on it. */
/* The semno is the same as the tx number on which a Tx is waiting */

int zgt_tx::setTx_semno(long tid, int semno)
{
    zgt_tx *txptr;

    txptr = get_tx(tid);
    
    if (txptr == NULL)
    {
        printf("\n:::ERROR:Txid %d wants to wait on sem:%d of tid:%d which does not exist\n", this->tid, semno, tid);
        fflush(stdout);
        exit(1);
    }
    
    if ((txptr->semno == -1)|| (txptr->semno == semno))
    {
        txptr->semno = semno;
        return(0);
    }
    else if (txptr->semno != semno)
    {
        #ifdef TX_DEBUG
            printf(":::ERROR Trying to wait on sem:%d, but on Tx:%d\n", semno, txptr->tid);
            fflush(stdout);
        #endif
        exit(1);
    }
    
    return(0);
}


void *start_operation(long tid, long count)
{
    pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]); // Lock mutex[t] to make other
                                                 // threads of same transaction wait

    while(ZGT_Sh->condset[tid] != count) // Wait if condset[t] is != count
    {
      pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
    }
}

/* Otherside of the start operation. */
/* Signals the conditional broadcast. */

void *finish_operation(long tid)
{
    ZGT_Sh->condset[tid]--; // Decrement condset[tid] for allowing the next op
    pthread_cond_broadcast(&ZGT_Sh->condpool[tid]); // Other waiting threads of same tx
    pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]);
}


