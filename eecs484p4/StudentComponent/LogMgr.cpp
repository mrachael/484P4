#include "LogMgr.h"
#include <sstream>
#include <iostream>

using namespace std;


/*
* Find the LSN of the most recent log record for this TX.
* If there is no previous log record for this TX, return 
* the null LSN.
* Rachael did this
*/
int LogMgr::getLastLSN(int txnum) 
{
	return tx_table[txnum].lastLSN;
}

/*
* Update the TX table to reflect the LSN of the most recent
* log entry for this transaction.
* Rachael did this
*/
void LogMgr::setLastLSN(int txnum, int lsn) 
{ 
	tx_table[txnum].lastLSN = lsn;
	return; 
}

/*
* Force log records up to and including the one with the
* maxLSN to disk. Don't forget to remove them from the
* logtail once they're written!
* Rachael did this
*/
void LogMgr::flushLogTail(int maxLSN) 
{ 
	std::stringstream str;
	for (auto record : logtail)
	{
		str << record->toString() << "\n";
	}
	logtail.clear();
	se->updateLog(str.str());
	return; 
}


/* 
* Run the analysis phase of ARIES.
*/
void LogMgr::analyze(vector <LogRecord*> log) { 

	/* Locate most recent checkpoint */
	int checkpointLSN = se->get_master();

	return; }

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
void LogMgr::checkpoint() 
{ 
	// 1. Begin checkpoint record is written to indicate start
	int beginLSN = se->nextLSN();
	logtail.push_back(new LogRecord(beginLSN, NULL_LSN, -1, BEGIN_CKPT));
	// 2. End checkpoint is constructed with the contents  
	// of the txn table and dirty page table and appended to the log
	int endLSN = se->nextLSN();
	logtail.push_back(new ChkptLogRecord(endLSN, beginLSN, -1, tx_table, dirty_page_table));
	// 2.5. Flush
	flushLogTail(endLSN);
	// 3. Master record w/ LSN of begin 
	se->store_master(beginLSN);
	return; 
}

/*
* Commit the specified transaction.
* Rachael did this
*/
void LogMgr::commit(int txid) 
{ 
	// The log record is appended to the log, and...
	int next = se->nextLSN();
	logtail.push_back(new LogRecord(next, tx_table[txid].lastLSN, txid, COMMIT));
	// the log tail is written to stable storage, up to the commit 
	flushLogTail(next);
	return; 
}

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
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
	cout << txid << " " << page_id << " " << offset << " " << input << endl;
	int next = se->nextLSN();
	int last;

	if (tx_table.find(txid) == tx_table.end())
		last = -1;
	else
		last = getLastLSN(txid);
	setLastLSN(txid, get); 
	cout << last << endl; //What the fuck even

	UpdateLogRecord newRecord(next, last, txid, page_id, offset, input, oldtext);
	logtail.push_back(&newRecord);

	if (dirty_page_table.find(page_id) == dirty_page_table.end())
		dirty_page_table[page_id] = next;

	txTableEntry t(last, U);
	tx_table[next] = t;

	return next;
}

/*
* Sets this.se to engine. 
*/
void LogMgr::setStorageEngine(StorageEngine* engine) { 
	this->se = engine;
	return;
}
