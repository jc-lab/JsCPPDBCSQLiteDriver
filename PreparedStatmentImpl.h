/**
 * @file	PreparedStatmentImpl.h
 * @class	JsCPPDBC::SQLiteDriverInternal::PreparedStatmentImpl
 * @author	Jichan (development@jc-lab.net / http://ablog.jc-lab.net/category/JsCPPDBC )
 * @date	2018/11/05
 * @copyright Apache License 2.0
 */

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
#pragma once
#endif

#ifndef __JSCPPDBC_SQLITEDRIVER_PREPAREDSTATMENTIMPL_H__
#define __JSCPPDBC_SQLITEDRIVER_PREPAREDSTATMENTIMPL_H__

#include "SQLiteDriver.h"
#include <JsCPPDBC/exception/SQLException.h>
#include <JsCPPDBC/PreparedStatment.h>
#include <sqlite3.h>

namespace JsCPPDBC {
	namespace SQLiteDriverInternal {
		class PreparedStatmentImpl : public PreparedStatment
		{
		private:
			SQLiteDriver *m_driver;
			sqlite3_stmt *m_stmt;
			int m_lastrc;

		public:
			PreparedStatmentImpl(SQLiteDriver *driver, Type type, const char *sql) throw(exception::SQLException);
			virtual ~PreparedStatmentImpl();

			int close() override;
			int reset(void) override;
			int execute(void) throw(exception::SQLException) override;
			int64_t insert_rowid(void) override;
			bool fetchRow(EntityBase *entity) throw(exception::SQLException);
			int fetchRows(EntityListBase *entityList) throw(exception::SQLException);
		};

	}
}

#endif /* __JSCPPDBC_SQLITEDRIVER_PREPAREDSTATMENTIMPL_H__ */
