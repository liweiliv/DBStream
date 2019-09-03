#pragma once
#include "message/record.h"
#include "meta/metaData.h"
#include "function.h"
#include "util/threadLocal.h"
namespace STORE
{
	namespace SHELL {
		static constexpr int MAX_EXPRESSION_LENGTH = 1024;
		class field {
		public:
			uint8_t type;
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord * row) const = 0;
			field(uint8_t type) :type(type) {}
			virtual ~field(){}
		};
		class columnFiled :public field {
		private:
			const META::columnMeta* column;
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord* row)const
			{
				switch (type)
				{
				case T_INT32:
					return (void*)(uint64_t)*(int32_t*)row->column(column->m_columnIndex);
				case T_UINT32:
					return (void*)(uint64_t) * (uint32_t*)row->column(column->m_columnIndex);
				case T_INT64:
				case T_DATETIME:
					return (void*)(uint64_t) * (int64_t*)row->column(column->m_columnIndex);
				case T_UINT64:
				case T_TIMESTAMP:
					return (void*)(uint64_t) * (int64_t*)row->column(column->m_columnIndex);
				case T_FLOAT:
				{
					float v = *(float*)row->column(column->m_columnIndex);
					return *(void**)&v;
				}
				case T_DOUBLE:
				{
					float v = *(double*)row->column(column->m_columnIndex);
					return *(void**)&v;
				}
				case T_UINT16:
					return (void*)(uint64_t) * (uint16_t*)row->column(column->m_columnIndex);
				case T_INT16:
					return (void*)(uint64_t) * (int16_t*)row->column(column->m_columnIndex);
				case T_UINT8:
					return (void*)(uint64_t) * (uint8_t*)row->column(column->m_columnIndex);
				case T_INT8:
					return (void*)(uint64_t) * (int8_t*)row->column(column->m_columnIndex);
				default:
					return row->column(column->m_columnIndex);
				}
			}
			columnFiled(const META::columnMeta* column):field(column->m_columnType),column(column){}
		};
		class functionFiled :public field
		{
		public:
			field** argvs;
			const function* func;
			functionFiled(field** argvs, const function* func) :field(func->returnValueType), argvs(argvs), func(func) {}
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord* row) const
			{
				return func->exec(argvs,row);
			}
		};
		class expressionField :public field
		{
		private:
			static threadLocal<field **>stack;
		public:
			field **list;
			uint32_t listSize;
			bool logicOrMath;
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord* row) const;
		};
	}
}
