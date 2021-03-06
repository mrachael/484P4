#include "LogMgr.h"
#include <set>
#include <sstream>

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
	if (record != logtail.end() && maxLSN < (*record)->getLSN())
		return;
	while (record != logtail.end() && (*record)->getLSN() <= maxLSN)
	{
		str << (*record)->toString();
		delete *record;
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
	// Find the lsn of the most recent checkpoint
	int checkpointLSN = se->get_master();
	int checkpointIndex = -1;

	// Find the checkpoint in the log
	for (int i = 0; i < log.size(); i++) {
		if (log[i]->getLSN() == checkpointLSN)
			checkpointIndex = i;
	}

	// If there is a checkpoint, retrieve the dirty page table and tx table
	// Otherwise leave the maps empty.
	if (checkpointLSN != -1) {
		dirty_page_table = ((ChkptLogRecord*)log[checkpointIndex+1])->getDirtyPageTable();
		tx_table = ((ChkptLogRecord*)log[checkpointIndex+1])->getTxTable();
	}

	// Scan log from checkpoint (if there is one) to the end of the log
	for(int i = checkpointIndex + 1; i < log.size(); i++) {
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
	int leastLSN = 0;
	if (dirty_page_table.begin() != dirty_page_table.end())
		leastLSN = dirty_page_table.begin()->second;
	
	for(auto it = dirty_page_table.begin(); it != dirty_page_table.end(); it++) {
		if (it->second < leastLSN)
			leastLSN = it->second;
	}

	int leastLSNLogIndex = 0;
	for (int i = 0; i < log.size(); i++)
	{
		if (log[i]->getLSN() == leastLSN)
		{
			leastLSNLogIndex = i;
			break;
		}
	}
	// Find all logs that must be redone
	for(int i = leastLSNLogIndex; i < log.size(); i++) {
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
				{
					return false;
				}
			}
		}
	}

	// Add end records for any tx with status C, and remove them from tx table
	for(auto it = tx_table.begin(); it != tx_table.end(); it++) {
		if (it->second.status == C) {
			int next = se->nextLSN();
			logtail.push_back(new LogRecord(next, it->second.lastLSN, it->first, END));
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
	set<int> ToUndo;
	if (txnum == NULL_TX)
	{
		for (auto txn : tx_table)
		{
			if (txn.second.status == U && txn.second.lastLSN != -1)
				ToUndo.insert(txn.second.lastLSN);
		}
	}
	else
	{
		ToUndo.insert(tx_table[txnum].lastLSN);
	}

	while (!ToUndo.empty())
	{
		int L = *(ToUndo.rbegin());

		LogRecord *record = nullptr;
		for (auto rec : log)
		{
			if (rec->getLSN() == L)
			{
				record = rec;
				break;
			}
		}
		if (!record)
		{
			return;
		}

		// If it is an update, write a CLR and undo the action
		// And add the prevLSN to the set toUndo
		if (record->getType() == UPDATE)
		{
			UpdateLogRecord *upRecord = (UpdateLogRecord*)record;
			int pageLSN = se->getLSN(upRecord->getPageID());

			int next = se->nextLSN();

			logtail.push_back(new CompensationLogRecord(
				next, 
				getLastLSN(upRecord->getTxID()), 
				upRecord->getTxID(), 
				upRecord->getPageID(), 
				upRecord->getOffset(),
				upRecord->getBeforeImage(),
				record->getprevLSN()));
			tx_table[upRecord->getTxID()].lastLSN = next;

			// Undo the action
			if (dirty_page_table.find(upRecord->getPageID()) != dirty_page_table.end() 
				&& pageLSN < record->getLSN() 
				&& dirty_page_table.find(upRecord->getPageID())->second <= record->getLSN()) 
			{
				bool unwritten = se->pageWrite(upRecord->getPageID(), upRecord->getOffset(), upRecord->getBeforeImage(), upRecord->getLSN());
				
				// Return false if Storage Engine got stuck
				if (!unwritten)
					return;
			}

			if (record->getprevLSN() != NULL_LSN)
				ToUndo.insert(record->getprevLSN());
			else
			{
				int next2 = se->nextLSN();
				logtail.push_back(new LogRecord(next2, next, record->getTxID(), END)); 
				tx_table.erase(record->getTxID());
			}

		}

		if (record->getType() == ABORT) {
			ToUndo.insert(record->getprevLSN());
		}

		// If it is a CLR
		if (record->getType() == CLR)
		{
			// If the undoNextLSN value is null, write an end record 
			if (((CompensationLogRecord*)record)->getUndoNextLSN() == NULL_LSN)
			{
				int next2 = se->nextLSN();
				logtail.push_back(new LogRecord(next2, record->getLSN(), record->getTxID(), END)); 
				tx_table.erase(record->getTxID());
			}
			else
				ToUndo.insert(((CompensationLogRecord*)record)->getUndoNextLSN()); 
		}

		ToUndo.erase(L);

	}

	return; 
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
	vector<LogRecord*> resultLogs;
	istringstream text(logstring);
	string line;

	while (getline(text, line)) {
		LogRecord* l = LogRecord::stringToRecordPtr(line);
		resultLogs.push_back(l);
	}

	return resultLogs;
}

/*
* Abort the specified transaction.
* Hint: you can use your undo function
*/
void LogMgr::abort(int txid) 
{ 
	if (tx_table.find(txid) == tx_table.end())
		return;

	int next = se->nextLSN();
	logtail.push_back(new LogRecord(next, tx_table[txid].lastLSN, txid, ABORT));

	string logOnDiskString = se->getLog();
	vector<LogRecord*> logOnDiskVector;
	logOnDiskVector = stringToLRVector(logOnDiskString);

	for (auto it = logtail.begin(); it != logtail.end(); it++)
		logOnDiskVector.push_back(*it);

	// Undo the transaction in the whole log
	setLastLSN(txid, next);
	undo(logOnDiskVector, txid);

	if (tx_table.find(txid) != tx_table.end())
		tx_table.erase(txid);

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
	setLastLSN(txid, next);
	tx_table.find(txid)->second.status = C;

	// the log tail is written to stable storage, up to the commit 
	flushLogTail(next);
	next = se->nextLSN();
	logtail.push_back(new LogRecord(next, tx_table[txid].lastLSN, txid, END));
	tx_table.erase(txid);

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
	
	dirty_page_table.erase(page_id);

	flushLogTail(lsn);

	return; 
}

/*
* Recover from a crash, given the log from the disk.
* Catherine did this
*/
void LogMgr::recover(string log) { 
	vector<LogRecord*> logs;
	logs = stringToLRVector(log);

	analyze(logs);
	redo(logs);
	undo(logs);

	return; 
}

/*
* Logs an update to the database and updates tables if needed.
* Catherine did this
*/
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
	int pageLSN = se->nextLSN();
	int last;

	// Determine the last LSN
	last = getLastLSN(txid);

	// Update the last LSN for this transaction
	setLastLSN(txid, pageLSN); 

	logtail.push_back(new UpdateLogRecord(pageLSN, last, txid, page_id, offset, oldtext, input));

	// Update dirty page table if necessary
	if (dirty_page_table.find(page_id) == dirty_page_table.end())
		dirty_page_table[page_id] = pageLSN;

	// Update transaction table
	txTableEntry t(pageLSN, U);
	tx_table[txid] = t;

	return pageLSN;
}

/*
* Sets this.se to engine. 
*/
void LogMgr::setStorageEngine(StorageEngine* engine) { 
	this->se = engine;
	return;
}
