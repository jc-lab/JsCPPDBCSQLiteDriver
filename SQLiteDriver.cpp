/**
 * @file	SQLiteDriver.cpp
 * @class	JsCPPDBC::SQLiteDriver
 * @author	Jichan (development@jc-lab.net / http://ablog.jc-lab.net/category/JsCPPDBC )
 * @date	2018/11/05
 * @copyright Apache License 2.0
 */

#include "SQLiteDriver.h"
#include "PreparedStatmentImpl.h"

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
				if (flag) {
					flag = false;
					sql.append("`");
					if (!iter->second.m_insertQuery.empty())
						sqlValuePart.append(iter->second.m_insertQuery);
					else
						sqlValuePart.append("?");

				}
				else {
					sql.append(", `");
					sqlValuePart.append(",");
					if (!iter->second.m_insertQuery.empty())
						sqlValuePart.append(iter->second.m_insertQuery);
					else
						sqlValuePart.append("?");
				}
				sql.append(iter->first);
				sql.append("`");
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
					if (!iter->second.m_updateQuery.empty())
						sql.append(iter->second.m_updateQuery);
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
					if (!iter->second.m_insertQuery.empty())
						sqlValuePart.append(iter->second.m_insertQuery);
					else
						sqlValuePart.append("?");
				}
				else {
					sql.append(", `");
					sqlValuePart.append(",");
					if (!iter->second.m_insertQuery.empty())
						sqlValuePart.append(iter->second.m_insertQuery);
					else
						sqlValuePart.append("?");
				}
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
			if (!(iter->second.isId && iter->second.generatedValue == EntityColumn::IDENTITY) && iter->second.m_insertable && iter->second.m_insertQuery.empty()) {
				stmt->addParam(iter->second);
			}
		}
	}

	void SQLiteDriver::addParamToStmtForUpdate(PreparedStatment *stmt, EntityBase *entity)
	{
		for (std::map<std::string, EntityColumn>::iterator iter = entity->_jsh_columns.begin(); iter != entity->_jsh_columns.end(); iter++) {
			if (!iter->second.isId && iter->second.m_updatable && iter->second.m_updateQuery.empty()) {
				stmt->addParam(iter->second);
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
			if (iter->second.m_insertable && iter->second.m_insertQuery.empty()) {
				stmt->addParam(iter->second);
			}
		}
	}


}
