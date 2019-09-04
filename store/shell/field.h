#pragma once
#include "message/record.h"
#include "meta/metaData.h"
#include "function.h"
#include "util/threadLocal.h"
#include "memory/bufferPool.h"
namespace STORE
{
	namespace SHELL {
		extern bufferPool shellGlobalBufferPool;
		static constexpr int MAX_EXPRESSION_LENGTH = 1024;
		class field {
		public:
			uint8_t type;
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord * row) const = 0;
			field(uint8_t type) :type(type) {}
			virtual ~field(){}
		};
		class rawField :public field {
			void* data;
			bool fromAlloc;
			rawField(void* data, uint8_t type,bool fromAlloc = false) :field(type), data(data) , fromAlloc(fromAlloc){}
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord* row) const
			{
				return data;
			}
			virtual ~rawField()
			{
				if (fromAlloc)
					basicBufferPool::free(data);
			}
		};
		struct varLenValue 
		{
			uint32_t size;
			bool alloced;
			const char* value;
		};
		class columnFiled :public field {
		private:
			const META::columnMeta* column;
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord* row)const
			{
				switch (type)
				{
				case META::T_INT32:
					return (void*)(uint64_t)*(int32_t*)row->column(column->m_columnIndex);
				case META::T_UINT32:
					return (void*)(uint64_t) * (uint32_t*)row->column(column->m_columnIndex);
				case META::T_INT64:
				case META::T_DATETIME:
					return (void*)(uint64_t) * (int64_t*)row->column(column->m_columnIndex);
				case META::T_UINT64:
				case META::T_TIMESTAMP:
					return (void*)(uint64_t) * (int64_t*)row->column(column->m_columnIndex);
				case META::T_FLOAT:
				{
					float v = *(float*)row->column(column->m_columnIndex);
					return *(void**)&v;
				}
				case META::T_DOUBLE:
				{
					float v = *(double*)row->column(column->m_columnIndex);
					return *(void**)&v;
				}
				case META::T_UINT16:
					return (void*)(uint64_t) * (uint16_t*)row->column(column->m_columnIndex);
				case META::T_INT16:
					return (void*)(uint64_t) * (int16_t*)row->column(column->m_columnIndex);
				case META::T_UINT8:
					return (void*)(uint64_t) * (uint8_t*)row->column(column->m_columnIndex);
				case META::T_INT8:
					return (void*)(uint64_t) * (int8_t*)row->column(column->m_columnIndex);
				default:
				{
					if (META::columnInfos[type].fixed)
						return (void*)row->column(column->m_columnIndex);
					else
					{
						varLenValue *v  = (varLenValue*)shellGlobalBufferPool.allocByLevel(0);
						v->alloced = false;
						v->size = row->varColumnSize(column->m_columnIndex);
						v->value = row->column(column->m_columnIndex);
						return v;
					}
				}
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
			uint16_t listSize;
			bool logicOrMath;
			virtual void* getValue(const DATABASE_INCREASE::DMLRecord* row) const;
		};
	}
}
