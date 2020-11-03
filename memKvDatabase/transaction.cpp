#include "transaction.h"
#include "row.h"
namespace KVDB {
	void transaction::newOperator(row* r)
	{
		m_rows.insert(r);
		m_vesionList.push_back(r->tail);
	}

	dsStatus& transaction::commit(logApplyFunc func)
	{
		if (unlikely(m_start))
			dsFailedAndLogIt(errorCode::TRANSACTION_NOT_START, "transaction not start", WARNING);
		if (func != nullptr)
			dsReturnIfFailed(func(m_vesionList));
		for (rowMap::iterator iter = m_rows.begin(); iter != m_rows.end(); iter++)
			(*iter)->commit();
		dsOk();
	}
	dsStatus& transaction::rollback()
	{
		if (unlikely(m_start))
			dsFailedAndLogIt(errorCode::TRANSACTION_NOT_START, "transaction not start", WARNING);
		m_vesionList.clear();
		for (rowMap::iterator iter = m_rows.begin(); iter != m_rows.end(); iter++)
			(*iter)->rollback();
		m_rows.clear();
		dsOk();
	}
}
