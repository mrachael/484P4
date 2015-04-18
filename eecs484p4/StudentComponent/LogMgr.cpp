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
	std::stringstream str;
	auto record = logtail.begin();
	while (record != logtail.end() && (*record)->getLSN() <= maxLSN)
	{
		str << (*record)->toString() << "\n";
		record++;
	}
	se->updateLog(str.str());
	logtail.erase(logtail.begin(), record + (record == logtail.end() ? 0 : 1));
	
	return; 
}


/* 
* Run the analysis phase of ARIES.
* Catherine did this
*/
void LogMgr::analyze(vector <LogRecord*> log) { 

	/* Locate most recent checkpoint */
	int checkpointLSN = se->get_master();

	// If there is a checkpoint, retrieve the dirty page table and tx table
	// Otherwise leave the maps empty.
	if (checkpointLSN != -1) {
		dirty_page_table = ((ChkptLogRecord*)log[checkpointLSN+1])->getDirtyPageTable();
		tx_table = ((ChkptLogRecord*)log[checkpointLSN+1])->getTxTable();
	}

	cout << dirty_page_table.size() << endl;
	
	// Scan log from checkpoint (if there is one) to the end of the log
	for(int i = checkpointLSN + 1; i < log.size(); i++) {
		int txid = log[i]->getTxID();
		int lsn = log[i]->getLSN();
		int pageid;

		// Start by adjusting tx table as necessary
		// If an END record for a txid is found, remove txid from tx table
		if (log[i]->getType() == END) {
			if (tx_table.find(txid) != tx_table.end())
				tx_table.erase(txid);
		} else {
			// Otherwise, if it isn't already in the tx table, add it
			TxStatus stat;
			if (log[i]->getType() == COMMIT)
				stat = C;
			else
				stat = U;

			if (tx_table.find(txid) == tx_table.end()) {
				txTableEntry t(lsn, stat);
				tx_table[txid] = t;
			} else {
				// If it is in the table, update it
				tx_table.find(txid)->second.lastLSN = lsn;
				tx_table.find(txid)->second.status = stat;
			}
		}
		// Adjust dirty_page_table as necessary
		// If an update record is found and the page is not in the dirty_page_table, add it
		if (log[i]->getType() == UPDATE || log[i]->getType() == CLR) {
			if (log[i]->getType() == UPDATE)
				pageid = ((UpdateLogRecord*)log[i])->getPageID();
			else
				pageid = ((CompensationLogRecord*)log[i])->getPageID();

			if (dirty_page_table.find(pageid) == dirty_page_table.end())
				dirty_page_table[pageid] = lsn;
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
	int checkLSN = se->get_master();

	// Find the least recLSN
	int leastLSN = 10000;
	for(auto it = dirty_page_table.begin(); it != dirty_page_table.end(); it++) {
		if (it->second < leastLSN)
			leastLSN = it->second;
	}

	// Find all logs that must be redone
	for(int i = leastLSN; i < log.size(); i++) {
		int lsn = log[i]->getLSN();
		int txid = log[i]->getTxID();

		if (log[i]->getType() == UPDATE || log[i]->getType() == CLR) {
			int pageid;
			int offSet;
			string text;	

			// Convert to the correct type and retrieve arguments
			if (log[i]->getType() == UPDATE) {
				pageid = ((UpdateLogRecord*)log[i])->getPageID();
				offSet = ((UpdateLogRecord*)log[i])->getOffset();
				text = ((UpdateLogRecord*)log[i])->getAfterImage();
			} else {
				pageid = ((CompensationLogRecord*)log[i])->getPageID();
				offSet = ((CompensationLogRecord*)log[i])->getOffset();
				text = ((CompensationLogRecord*)log[i])->getAfterImage();
			}
			int pageLSN = se->getLSN(pageid);

			// Reapply, if necessary
			if (dirty_page_table.find(pageid) != dirty_page_table.end() && 
				pageLSN < lsn && dirty_page_table.find(pageid)->second <= lsn) {
				bool written = se->pageWrite(pageid, offSet, text, lsn);
				
				// Return false is Storage Engine got stuck
				if (!written)
					return false;
			}
		}
	}

	// Add end records for any tx with status C, and remove them from tx table
	for(auto it = tx_table.begin(); it != tx_table.end(); it++) {
		if (it->second.status == C) {
			logtail.push_back(new LogRecord(se->nextLSN(), it->second.lastLSN, it->first, END));
			tx_table.erase(it->first);
		}
	}

	return true;
}

/*
* If no txnum is specified, run the undo phase of ARIES.
* If a txnum is provided, abort that transaction.
* Hint: the logic is very similar for these two tasks!
*/
void LogMgr::undo(vector <LogRecord*> log, int txnum) 
{ 
	/*while (!log.empty())
	{
		LogRecord* record = log[log.size()-1];

		// If it is a CLR
		if (record->getType() == CLR)
		{
			// If the undoNextLSN value is null, write an end record 
			if (CompensationLogRecord*)record->undoNextLSN() == NULL)
				logtail.push_back(se->nextLSN(), record->getLSN(), record->tx_id, END); 
		} 
		// If it is an update, write a CLR and undo the action
		// And add the prevLSN to the set toUndo??
		if (record->getType() == UPDATE)
		{
			logtail.push_back
		}

		log.pop_back();
	}*/

	return; 
}

/*
* Abort the specified transaction.
* Hint: you can use your undo function
*/
void LogMgr::abort(int txid) 
{ 
	undo(logtail, txid);
	return; 
}

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
	cout << log << endl;
	vector<string> logString;
	vector<LogRecord*> logs;

	int newLine = log.find("\n");
	while (newLine != -1) {
		logString.push_back(log.substr(0, newLine));
		log = log.substr(newLine + 1, log.size());
		newLine = log.find("\n");
	}

	string args[8];
	for (auto it = logString.begin(); it != logString.end(); it++) {
		int txid, lsn, prevLSN, pageid, offset;
		string before, after;
		map<int, int> DPT;
		map<int, txTableEntry> Tx;
		
		int tab = it->find("\t");
		int i = 0;
		while (tab != -1 && i < 8) {
			args[i] = it->substr(0, tab);
			*it = it->substr(tab + 1, it->size());
			tab = it->find("\t");
		}

		lsn = atoi(args[0].c_str());
		prevLSN = atoi(args[1].c_str());
		txid = atoi(args[2].c_str());
		if (it->find("update") != -1) {
			pageid = atoi(args[4].c_str());
			offset = atoi(args[5].c_str());
			before = args[6];
			after = args[7];
			logs.push_back(new UpdateLogRecord(lsn, prevLSN, txid, pageid, offset, before, after));
		} else if (it->find("commit")) {
			logs.push_back(new LogRecord(lsn, prevLSN, txid, COMMIT));
		} else if (it->find("abort")) {
			logs.push_back(new LogRecord(lsn, prevLSN, txid, ABORT));
		} else if (it->find("end")) {
			logs.push_back(new LogRecord(lsn, prevLSN, txid, END));
		} else if (it->find("checkpoint")) {
			logs.push_back(new ChkptLogRecord(lsn, prevLSN, txid, Tx, DPT));
		}
	}

	cout << "go!\n";
	analyze(logs);
	cout << "go!\n";
	redo(logs);
	cout << "go!\n";
	undo(logs);
	cout << "go!\n";

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
	last = getLastLSN(txid);

	// Update the last LSN for this transaction
	setLastLSN(txid, next); 

	logtail.push_back(new UpdateLogRecord(next, last, txid, page_id, offset, oldtext, input));

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
