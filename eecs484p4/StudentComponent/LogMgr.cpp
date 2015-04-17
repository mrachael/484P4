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
	if (tx_table.find(txnum) == tx_table.end())
		return NULL_LSN;
	return tx_table[txnum].lastLSN;
}

/*
* Update the TX table to reflect the LSN of the most recent
* log entry for this transaction.
* Rachael did this
*/
void LogMgr::setLastLSN(int txnum, int lsn) 
{
	if (tx_table.find(txnum) == tx_table.end())
		return; 
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
	//std::stringstream str;
	for (int i = 0; i < logtail.size(); i++) //record : logtail)
	{
		//str << record->toString() << "\n";
		se->updateLog(logtail[i]->toString() + "\n");
	}
	logtail.clear();
	
	return; 
}


/* 
* Run the analysis phase of ARIES.
* Catherine did this
*/
void LogMgr::analyze(vector <LogRecord*> log) { 

	/* Locate most recent checkpoint */
	int checkpointLSN = se->get_master();
	map<int, int> *DPT;
	map<int, txTableEntry> *Tx; 

	// If there is a checkpoint, retrieve the dirty page table and tx table
	// Otherwise leave the maps empty.
	if (checkpointLSN != -1) {
		*DPT = ((ChkptLogRecord*)log[checkpointLSN+1])->getDirtyPageTable();
		*Tx = ((ChkptLogRecord*)log[checkpointLSN+1])->getTxTable();
	}
	
	// Scan log from checkpoint (if there is one) to the end of the log
	for(int i = checkpointLSN + 1; i < log.size(); i++) {
		int txid = log[i]->getTxID();
		int lsn = log[i]->getLSN();
		int pageid;

		// Start by adjusting tx table as necessary
		// If an END record for a txid is found, remove txid from tx table
		if (log[i]->getType() == END) {
			if (Tx->find(txid) != Tx->end())
				Tx->erase(txid);
		} else {
			// Otherwise, if it isn't already in the tx table, add it
			TxStatus stat;
			if (log[i]->getType() == COMMIT)
				stat = C;
			else
				stat = U;

			if (Tx->find(txid) == Tx->end()) {
				txTableEntry t(lsn, stat);
				(*Tx)[txid] = t;
			} else {
				// If it is in the table, update it
				Tx->find(txid)->second.lastLSN = lsn;
				Tx->find(txid)->second.status = stat;
			}
		}

		// Adjust DPT as necessary
		// If an update record is found and the page is not in the DPT, add it
		if (log[i]->getType() == UPDATE) {
			pageid = ((UpdateLogRecord*)log[i])->getPageID();
			if (DPT->find(pageid) == DPT->end())
				(*DPT)[pageid] = lsn;
		}
	}

	return;
}

/*
* Run the redo phase of ARIES.
* If the StorageEngine stops responding, return false.
* Else when redo phase is complete, return true. 
* Catherine did this
*/

bool LogMgr::redo(vector <LogRecord*> log) { 
	map<int, int> *DPT;
	map<int, txTableEntry> *Tx;
	int checkLSN = se->get_master();

	*DPT = ((ChkptLogRecord*)log[checkLSN+1])->getDirtyPageTable();
	*Tx = ((ChkptLogRecord*)log[checkLSN+1])->getTxTable();

	// Find the least recLSN
	int leastLSN = 10000;
	for(auto it = DPT->begin(); it != DPT->end(); it++) {
		if (it->second < leastLSN)
			leastLSN = it->second;
	}

	// Find all logs that must be redone
	for(int i = leastLSN; i < log.size(); i++) {
		int lsn = log[i]->getLSN();
		int txid = log[i]->getTxID();

		if (log[i]->getType() == UPDATE) {
			int pageid = ((UpdateLogRecord*)log[i])->getPageID();
			int offSet = ((UpdateLogRecord*)log[i])->getOffset();
			string text = ((UpdateLogRecord*)log[i])->getAfterImage();
			int pageLSN = se->getLSN(pageid);

			// Reapply, if necessary
			if (DPT->find(pageid) != DPT->end() && pageLSN < lsn && DPT->find(pageid)->second <= lsn) {
				bool written = se->pageWrite(pageid, offSet, text, lsn);
				
				// Return false is Storage Engine got stuck
				if (!written)
					return false;
			}
		}
	}

	// Add end records for any tx with status C, and remove them from tx table
	for(auto it = Tx->begin(); it != Tx->end(); it++) {
		if (it->second.status == C) {
			logtail.push_back(new LogRecord(se->nextLSN(), it->second.lastLSN, it->first, END));
			Tx->erase(it->first);
		}
	}

	return true;
}

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
	if (tx_table.find(txid) == tx_table.end())
		return;
	logtail.push_back(new LogRecord(next, tx_table[txid].lastLSN, txid, COMMIT));
	// the log tail is written to stable storage, up to the commit 
	flushLogTail(next);
	return; 
}

/*
* A function that StorageEngine will call when it's about to 
* write a page to disk. 
* Remember, you need to implement write-ahead logging
* Catherine did this
*/
void LogMgr::pageFlushed(int page_id) {
	// Get LSN matching the page
	int lsn = se->getLSN(page_id); 
	

	/*vector<LogRecord*>::iterator it = logtail.begin();
	for (it; it != logtail.end(); it++) {
		if ((*it)->getLSN() == lsn && (*it)->getType() == UPDATE)
			cout << "Wheeeee\n";
	}*/
	return; 
}

/*
* Recover from a crash, given the log from the disk.
* Catherine did this
*/
void LogMgr::recover(string log) { 
	cout << "okay but fucking why not" << endl;
	return; 
}

/*
* Logs an update to the database and updates tables if needed.
* Catherine did this
*/
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
	int next = se->nextLSN();
	int last;

	// Determine the last LSN
	if (tx_table.find(txid) == tx_table.end())
		last = -1;
	else
		last = getLastLSN(txid);

	// Update the last LSN for this transaction
	setLastLSN(txid, next); 

	logtail.push_back(new UpdateLogRecord(next, last, txid, page_id, offset, input, oldtext));

	// Update dirty page table if necessary
	if (dirty_page_table.find(page_id) == dirty_page_table.end())
		dirty_page_table[page_id] = next;

	// Update transaction table
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
