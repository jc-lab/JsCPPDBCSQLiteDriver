/**
 * @file	SQLiteDriver.cpp
 * @class	JsCPPDBC::SQLiteDriver
 * @author	Jichan (development@jc-lab.net / http://ablog.jc-lab.net/category/JsCPPDBC )
 * @date	2018/11/05
 * @copyright Apache License 2.0
 */

#include "SQLiteDriver.h"
#include "PreparedStatmentImpl.h"

#undef _HAS_SLEEP
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#define _HAS_SLEEP 1
#undef Sleep
#define Sleep(x) usleep((x)*1000)
#elif defined(_WIN32)
#include <windows.h>
#define _HAS_SLEEP 1
#endif

namespace JsCPPDBC {

	SQLiteDriver::SQLiteDriver()
	{
		m_conn = NULL;
	}


	SQLiteDriver::~SQLiteDriver()
	{
		close();
	}

	Ptr<SQLiteDriver> SQLiteDriver::createInstance()
	{
		Ptr<SQLiteDriver> object(new SQLiteDriver());
		return object;
	}

	int SQLiteDriver::open(const char *filepath, int sqliteOpenFlags)
	{
		int nrst;
		nrst = sqlite3_open_v2(filepath, &m_conn, sqliteOpenFlags, NULL);
		return nrst;
	}

	int SQLiteDriver::execute(const char *query)
	{
		int rc;
		char *pszErrmsg = NULL;
		rc = sqlite3_exec(m_conn, query, NULL, NULL, &pszErrmsg);
		m_lasterrno = rc;
		if (pszErrmsg)
			m_lasterrmsg = pszErrmsg;
		else
			m_lasterrmsg.clear();
		return rc;
	}

	int SQLiteDriver::close()
	{
		int rc = sqlite3_close(m_conn);
		m_conn = NULL;
		return rc;
	}

	Ptr<PreparedStatment> SQLiteDriver::createSQLQuery(PreparedStatment::Type type, const char *sql)
	{
		return Ptr<PreparedStatment>(new SQLiteDriverInternal::PreparedStatmentImpl(this, type, sql));
	}

	Ptr<PreparedStatment> SQLiteDriver::createInsertStmt(EntityBase *entity)
	{
		std::string sql = "INSERT INTO `";
		std::string sqlValuePart;
		bool flag;

		sql.append(entity->getTblName());
		sql.append("` (");

		flag = true;
		for (std::map<std::string, EntityColumn>::const_iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (!(iter->second.isId && iter->second.generatedValue == EntityColumn::IDENTITY) && iter->second.m_insertable) {
				if (flag) 
					flag = false;
				else {
					sql.append(",");
					sqlValuePart.append(",");
				}
				sql.append("`");
				sql.append(iter->first);
				sql.append("`");
				if (!iter->second.m_defaultInsertQuery.empty()) {
					sqlValuePart.append("CASE WHEN ? IS NULL OR ?=='' then ");
					sqlValuePart.append(iter->second.m_defaultInsertQuery);
					sqlValuePart.append(" else ? END");
				}
				else
					sqlValuePart.append("?");
			}
		}
		sql.append(") VALUES (");
		sql.append(sqlValuePart);
		sql.append(")");

		return createSQLQuery(PreparedStatment::TYPE_UPDATE, sql.c_str());
	}

	Ptr<PreparedStatment> SQLiteDriver::createUpdateStmt(EntityBase *entity)
	{
		std::string sql = "UPDATE `";
		bool flag;

		sql.append(entity->getTblName());
		sql.append("` SET ");

		flag = true;
		for (std::map<std::string, EntityColumn>::const_iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (iter->second.m_updatable) {
				if (!iter->second.isId) {
					if (flag) {
						flag = false;
					}
					else {
						sql.append(",");
					}
					sql.append("`");
					sql.append(iter->first);
					sql.append("`=");

					if (!iter->second.m_defaultUpdateQuery.empty()) {
						sql.append("CASE WHEN ? IS NULL OR ?=='' then ");
						sql.append(iter->second.m_defaultUpdateQuery);
						sql.append(" else ? END");
					}
					else
						sql.append("?");
				}
			}
		}

		for (std::map<std::string, EntityColumn>::const_iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (iter->second.isId) {
				sql.append(" WHERE `");
				sql.append(iter->first);
				sql.append("`=?");
				break;
			}
		}

		return createSQLQuery(PreparedStatment::TYPE_UPDATE, sql.c_str());
	}

	Ptr<PreparedStatment> SQLiteDriver::createSaveStmt(EntityBase *entity)
	{
		std::string sql = "INSERT OR REPLACE INTO `";
		std::string sqlValuePart;
		bool flag;

		sql.append(entity->getTblName());
		sql.append("` (");

		flag = true;
		for (std::map<std::string, EntityColumn>::const_iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (iter->second.m_insertable) {
				if (flag) {
					flag = false;
					sql.append("`");
				}
				else {
					sql.append(", `");
					sqlValuePart.append(",");
				}

				if (!iter->second.m_defaultInsertQuery.empty()) {
					sqlValuePart.append("CASE WHEN ? IS NULL OR ?=='' then ");
					sqlValuePart.append(iter->second.m_defaultInsertQuery);
					sqlValuePart.append(" else ? END");
				}
				else
					sqlValuePart.append("?");

				sql.append(iter->first);
				sql.append("`");
			}
		}
		sql.append(") VALUES (");
		sql.append(sqlValuePart);
		sql.append(")");

		return createSQLQuery(PreparedStatment::TYPE_UPDATE, sql.c_str());
	}

	void SQLiteDriver::addParamToStmtForInsert(PreparedStatment *stmt, EntityBase *entity)
	{
		for (std::map<std::string, EntityColumn>::iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (!(iter->second.isId && iter->second.generatedValue == EntityColumn::IDENTITY) && iter->second.m_insertable) {
				stmt->addParam(iter->second);
				if (!iter->second.m_defaultInsertQuery.empty())
				{
					stmt->addParam(iter->second); stmt->addParam(iter->second);
				}
			}
		}
	}

	void SQLiteDriver::addParamToStmtForUpdate(PreparedStatment *stmt, EntityBase *entity)
	{
		for (std::map<std::string, EntityColumn>::iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (!iter->second.isId && iter->second.m_updatable) {
				stmt->addParam(iter->second);
				if (!iter->second.m_defaultUpdateQuery.empty())
				{
					stmt->addParam(iter->second); stmt->addParam(iter->second);
				}
			}
		}

		for (std::map<std::string, EntityColumn>::const_iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (iter->second.isId) {
				stmt->addParam(iter->second);
				break;
			}
		}
	}

	void SQLiteDriver::addParamToStmtForSave(PreparedStatment *stmt, EntityBase *entity)
	{
		for (std::map<std::string, EntityColumn>::iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (iter->second.m_insertable) {
				stmt->addParam(iter->second);
				if (!iter->second.m_defaultInsertQuery.empty())
				{
					stmt->addParam(iter->second); stmt->addParam(iter->second);
				}
			}
		}
	}

	int SQLiteDriver::flush(int nUseRetry, int retryTimes, int retryTimeMs)
	{
		if (m_conn)
		{
			int i;
			int nrst;
			i = 0;
			do {
				nrst = sqlite3_wal_checkpoint_v2(m_conn, NULL, SQLITE_CHECKPOINT_RESTART, NULL, NULL);
				if (((nrst == SQLITE_BUSY) || (nrst == SQLITE_LOCKED)) && (nUseRetry == 1))
				{
#if defined(_HAS_SLEEP) && _HAS_SLEEP
					::Sleep(retryTimeMs);
#endif
					i++;
				}
				else
					break;
			} while (i < retryTimes);
			return nrst;
		}
		return -1;
	}
}
