#include "appendingBlock.h"
namespace DATABASE {
	iterator* createAppendingBlockIndexIterator(appendingBlock* block, appendingIndex* index)
	{
		switch (index->getType())
		{
		case META::COLUMN_TYPE::T_UNION:
			return new appendingBlockIndexIterator<unionKey>(block, index);
		case META::COLUMN_TYPE::T_INT8:
			return new appendingBlockIndexIterator<int8_t>(block, index);
		case META::COLUMN_TYPE::T_UINT8:
			return new appendingBlockIndexIterator<uint8_t>(block, index);
		case META::COLUMN_TYPE::T_INT16:
			return new appendingBlockIndexIterator<int16_t>(block, index);
		case META::COLUMN_TYPE::T_UINT16:
			return new appendingBlockIndexIterator<uint16_t>(block, index);
		case META::COLUMN_TYPE::T_INT32:
			return new appendingBlockIndexIterator<int32_t>(block, index);
		case META::COLUMN_TYPE::T_UINT32:
			return new appendingBlockIndexIterator<uint32_t>(block, index);
		case META::COLUMN_TYPE::T_INT64:
			return new appendingBlockIndexIterator<int64_t>(block, index);
		case META::COLUMN_TYPE::T_TIMESTAMP:
		case META::COLUMN_TYPE::T_UINT64:
			return new appendingBlockIndexIterator<uint64_t>(block, index);
		case META::COLUMN_TYPE::T_FLOAT:
			return new appendingBlockIndexIterator<float>(block, index);
		case META::COLUMN_TYPE::T_DOUBLE:
			return new appendingBlockIndexIterator<double>(block, index);
		case META::COLUMN_TYPE::T_BLOB:
		case META::COLUMN_TYPE::T_STRING:
			return new appendingBlockIndexIterator<binaryType>(block, index);
		default:
			abort();
		}
	}

}