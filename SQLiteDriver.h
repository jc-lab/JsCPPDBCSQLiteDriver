/**
 * @file	SQLiteDriver.h
 * @class	JsCPPDBC::SQLiteDriver
 * @author	Jichan (development@jc-lab.net / http://ablog.jc-lab.net/category/JsCPPDBC )
 * @date	2018/11/05
 * @copyright Apache License 2.0
 */

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
#pragma once
#endif

#ifndef __JSCPPDBC_SQLITEDRIVER_SQLITEDRIVER_H__
#define __JSCPPDBC_SQLITEDRIVER_SQLITEDRIVER_H__

#include <JsCPPDBC/Ptr.h>
#include <JsCPPDBC/exception/SQLException.h>
#include <JsCPPDBC/DriverBase.h>

#include <sqlite3.h>

#include <string>

namespace JsCPPDBC {

	class SQLiteDriver : public JsCPPDBC::DriverBase
	{
	private:
		sqlite3 *m_conn;
		int m_lasterrno;
		std::string m_lasterrmsg;

	public:
		virtual ~SQLiteDriver();
		static Ptr<SQLiteDriver> createInstance();

		sqlite3 *getNativeConnection() {
			return m_conn;
		}

		int open(const char *filepath, int sqliteOpenFlags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_NOMUTEX);
		int execute(const char *query) override;
		int close() override;

		Ptr<PreparedStatment> createSQLQuery(PreparedStatment::Type type, const char *sql) throw (exception::SQLException) override;

		Ptr<PreparedStatment> createInsertStmt(EntityBase *entity) override;
		Ptr<PreparedStatment> createUpdateStmt(EntityBase *entity) override;
		Ptr<PreparedStatment> createSaveStmt(EntityBase *entity) override;
		void addParamToStmtForInsert(PreparedStatment *stmt, EntityBase *entity) override;
		void addParamToStmtForUpdate(PreparedStatment *stmt, EntityBase *entity) override;
		void addParamToStmtForSave(PreparedStatment *stmt, EntityBase *entity) override;

		int flush(int nUseRetry = 1, int retryTimes = 3, int retryTimeMs = 100);

	private:
		SQLiteDriver();
	};

}

#endif /* __JSCPPDBC_SQLITEDRIVER_SQLITEDRIVER_H__ */
