#include "LogMgr.h"
#include <sstream>

using namespace std;


/*
* Find the LSN of the most recent log record for this TX.
* If there is no previous log record for this TX, return 
* the null LSN.
*/
int LogMgr::getLastLSN(int txnum) { return 0; }

/*
* Update the TX table to reflect the LSN of the most recent
* log entry for this transaction.
*/
void LogMgr::setLastLSN(int txnum, int lsn) { return; }

/*
* Force log records up to and including the one with the
* maxLSN to disk. Don't forget to remove them from the
* logtail once they're written!
*/
void LogMgr::flushLogTail(int maxLSN) { return; }


/* 
* Run the analysis phase of ARIES.
*/
void LogMgr::analyze(vector <LogRecord*> log) { return; }

/*
* Run the redo phase of ARIES.
* If the StorageEngine stops responding, return false.
* Else when redo phase is complete, return true. 
*/

bool LogMgr::redo(vector <LogRecord*> log) { return true; }

/*
* If no txnum is specified, run the undo phase of ARIES.
* If a txnum is provided, abort that transaction.
* Hint: the logic is very similar for these two tasks!
*/
void LogMgr::undo(vector <LogRecord*> log, int txnum) { return; }

/*
* Abort the specified transaction.
* Hint: you can use your undo function
*/
void LogMgr::abort(int txid) { return; }

/*
* Write the begin checkpoint and end checkpoint
*/
void LogMgr::checkpoint() { return; }

/*
* Commit the specified transaction.
*/
void LogMgr::commit(int txid) { return; }

/*
* A function that StorageEngine will call when it's about to 
* write a page to disk. 
* Remember, you need to implement write-ahead logging
*/
void LogMgr::pageFlushed(int page_id) { return; }

/*
* Recover from a crash, given the log from the disk.
*/
void LogMgr::recover(string log) { return; }

/*
* Logs an update to the database and updates tables if needed.
*/
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) { return 0; }

/*
* Sets this.se to engine. 
*/
void LogMgr::setStorageEngine(StorageEngine* engine) { return; }