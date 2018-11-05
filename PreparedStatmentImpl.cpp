/**
 * @file	PreparedStatmentImpl.cpp
 * @class	JsCPPDBC::SQLiteDriverInternal::PreparedStatmentImpl
 * @author	Jichan (development@jc-lab.net / http://ablog.jc-lab.net/category/JsCPPDBC )
 * @date	2018/11/05
 * @copyright Apache License 2.0
 */

#include "PreparedStatmentImpl.h"
#include <JsCPPDBC/EntityBase.h>
#include <JsCPPDBC/EntityColumn.h>
#include <JsCPPDBC/exception/SQLException.h>

namespace JsCPPDBC {
	namespace SQLiteDriverInternal {

		PreparedStatmentImpl::PreparedStatmentImpl(SQLiteDriver *driver, Type type, const char *sql) :
			PreparedStatment(type, sql),
			m_driver(driver),
			m_stmt(NULL)
		{
			sqlite3 *db = m_driver->getNativeConnection();
			int nrst;
			m_lastrc = nrst = sqlite3_prepare(db, sql, -1, &m_stmt, NULL);
			if (nrst) {
				throw exception::SQLException(sqlite3_errmsg(db), nrst);
			}
		}

		PreparedStatmentImpl::~PreparedStatmentImpl()
		{
			close();
		}

		int PreparedStatmentImpl::close(void)
		{
			m_lastrc = -1;
			if (m_stmt) {
				sqlite3_finalize(m_stmt);
				m_stmt = NULL;
			}
			return 0;
		}

		int PreparedStatmentImpl::execute(void)
		{
			int nrst = 0;
			sqlite3 *db = m_driver->getNativeConnection();
			int paramidx = 0;
			for (std::list<EntityColumn>::const_iterator iter = m_params.begin(); iter != m_params.end(); iter++) {
				switch (iter->type) {
				case EntityColumn::TYPE_BOOL:
				{
					bool *value = (bool*)iter->buffer;
					sqlite3_bind_int(m_stmt, ++paramidx, *value ? 1 : 0);
					break;
				}
				case EntityColumn::TYPE_SINT8:
				case EntityColumn::TYPE_UINT8:
				{
					int8_t *value = (int8_t*)iter->buffer;
					sqlite3_bind_int(m_stmt, ++paramidx, ((int)*value) & 0xFF);
					break;
				}
				case EntityColumn::TYPE_SINT16:
				case EntityColumn::TYPE_UINT16:
				{
					int16_t *value = (int16_t*)iter->buffer;
					sqlite3_bind_int(m_stmt, ++paramidx, ((int)*value) & 0xFFFF);
					break;
				}
				case EntityColumn::TYPE_SINT32:
				case EntityColumn::TYPE_UINT32:
				{
					int32_t *value = (int32_t*)iter->buffer;
					sqlite3_bind_int(m_stmt, ++paramidx, *value);
					break;
				}
				case EntityColumn::TYPE_SINT64:
				case EntityColumn::TYPE_UINT64:
				{
					int64_t *value = (int64_t*)iter->buffer;
					sqlite3_bind_int64(m_stmt, ++paramidx, *value);
					break;
				}
				case EntityColumn::TYPE_FLOAT:
				{
					float *value = (float*)iter->buffer;
					sqlite3_bind_double(m_stmt, ++paramidx, *value);
					break;
				}
				case EntityColumn::TYPE_DOUBLE:
				{
					double *value = (double*)iter->buffer;
					sqlite3_bind_double(m_stmt, ++paramidx, *value);
					break;
				}
				case EntityColumn::TYPE_STRING:
				{
					std::string *value = (std::string*)iter->buffer;
					sqlite3_bind_text(m_stmt, ++paramidx, value->c_str(), value->length(), SQLITE_STATIC);
					break;
				}
				case EntityColumn::TYPE_BLOB_VECTOR:
				{
					std::vector<char> *value = (std::vector<char>*)iter->buffer;
					sqlite3_bind_text(m_stmt, ++paramidx, &(*value)[0], value->size(), SQLITE_STATIC);
					break;
				}
				case EntityColumn::TYPE_BLOB_BUF:
				{
					void *value = (void*)iter->buffer;
					sqlite3_bind_blob(m_stmt, ++paramidx, value, iter->size, SQLITE_STATIC);
					break;
				}
				}
			}
			if (m_type == TYPE_UPDATE) {
				m_lastrc = nrst = sqlite3_step(m_stmt);
				if (nrst != 0 && nrst != SQLITE_DONE) {
					throw exception::SQLException(sqlite3_errmsg(db), nrst);
				}
			}
			return nrst;
		}

		int64_t PreparedStatmentImpl::insert_rowid(void)
		{
			sqlite3 *db = m_driver->getNativeConnection();
			return sqlite3_last_insert_rowid(db);
		}

		bool PreparedStatmentImpl::fetchRow(EntityBase *entity)
		{
			sqlite3 *db = m_driver->getNativeConnection();
			int nrst;

			if ((nrst = sqlite3_step(m_stmt)) == SQLITE_ROW)
			{
				int i;
				int columns = sqlite3_column_count(m_stmt);
				for (i = 0; i < columns; i++) {
					const char *colname = sqlite3_column_name(m_stmt, i);
					std::map<std::string, EntityColumn>::iterator iterCol = entity->_jsh_columns.find(colname);
					if (iterCol != entity->_jsh_columns.end()) {
						switch (iterCol->second.type) {
						case EntityColumn::TYPE_BOOL:
						{
							int value = sqlite3_column_int(m_stmt, i);
							bool *buffer = (bool*)iterCol->second.buffer;
							*buffer = value ? true : false;
							break;
						}
						case EntityColumn::TYPE_SINT8:
						case EntityColumn::TYPE_UINT8:
						{
							int value = sqlite3_column_int(m_stmt, i);
							int8_t *buffer = (int8_t*)iterCol->second.buffer;
							*buffer = (int8_t)value;
							break;
						}
						case EntityColumn::TYPE_SINT16:
						case EntityColumn::TYPE_UINT16:
						{
							int value = sqlite3_column_int(m_stmt, i);
							int16_t *buffer = (int16_t*)iterCol->second.buffer;
							*buffer = (int16_t)value;
							break;
						}
						case EntityColumn::TYPE_SINT32:
						case EntityColumn::TYPE_UINT32:
						{
							int value = sqlite3_column_int(m_stmt, i);
							int32_t *buffer = (int32_t*)iterCol->second.buffer;
							*buffer = (int32_t)value;
							break;
						}
						case EntityColumn::TYPE_SINT64:
						case EntityColumn::TYPE_UINT64:
						{
							int64_t value = sqlite3_column_int64(m_stmt, i);
							int64_t *buffer = (int64_t*)iterCol->second.buffer;
							*buffer = value;
							break;
						}
						case EntityColumn::TYPE_FLOAT:
						{
							float *buffer = (float*)iterCol->second.buffer;
							*buffer = (float)sqlite3_column_double(m_stmt, i);
							break;
						}
						case EntityColumn::TYPE_DOUBLE:
						{
							double *buffer = (double*)iterCol->second.buffer;
							*buffer = sqlite3_column_double(m_stmt, i);
							break;
						}
						case EntityColumn::TYPE_STRING:
						{
							const char *value = (const char*)sqlite3_column_text(m_stmt, i);
							int length = sqlite3_column_bytes(m_stmt, i);
							std::string *buffer = (std::string*)iterCol->second.buffer;
							*buffer = std::string(value, length);
							break;
						}
						case EntityColumn::TYPE_BLOB_VECTOR:
						{
							const void *value = sqlite3_column_blob(m_stmt, i);
							int length = sqlite3_column_bytes(m_stmt, i);
							std::vector<char> *buffer = (std::vector<char>*)iterCol->second.buffer;
							buffer->clear();
							buffer->assign(length, 0);
							memcpy(&(*buffer)[0], value, length);
							break;
						}
						case EntityColumn::TYPE_BLOB_BUF:
						{
							const void *value = sqlite3_column_blob(m_stmt, i);
							int length = sqlite3_column_bytes(m_stmt, i);
							void *buffer = (void*)iterCol->second.buffer;
							int copysize = iterCol->second.size < length ? iterCol->second.size : length;
							memcpy(buffer, value, copysize);
							break;
						}
						}
					}
				}
				return true;
			}
			else if(nrst != SQLITE_DONE) {
				throw exception::SQLException(sqlite3_errmsg(db), nrst);
			}
			return false;
		}

		int PreparedStatmentImpl::fetchRows(EntityListBase *entityList)
		{
			Ptr<EntityBase> entity = entityList->createEntity();
			while (fetchRow(entity.getPtr())) {
				entityList->list.push_back(entity);
				entity = entityList->createEntity();
			}
			return 0;
		}
	}
}
