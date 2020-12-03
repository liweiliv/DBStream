#include "transaction.h"
#include "row.h"
namespace KVDB {
	void transaction::newOperator(row* r)
	{
		m_rows.insert(r);
		if (m_redoListTail != nullptr)
			m_redoListTail->transNext = r->tail;
		m_redoListTail = r->tail;
	}
	void transaction::clear()
	{
		m_rows.clear();
		m_tables.clear();
	}
	dsStatus& transaction::commit()
	{
		if (unlikely(m_start))
			dsFailedAndLogIt(errorCode::TRANSACTION_NOT_START, "transaction not start", WARNING);
		for (rowMap::iterator iter = m_rows.begin(); iter != m_rows.end(); iter++)
			(*iter)->commit();
		m_start = false;
		dsOk();
	}
	dsStatus& transaction::rollback()
	{
		if (unlikely(m_start))
			dsFailedAndLogIt(errorCode::TRANSACTION_NOT_START, "transaction not start", WARNING);
		for (rowMap::iterator iter = m_rows.begin(); iter != m_rows.end(); iter++)
			(*iter)->rollback();
		m_rows.clear();
		m_start = false;
		dsOk();
	}
}
