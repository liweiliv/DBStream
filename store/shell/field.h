#pragma once
#include "message/record.h"
#include "meta/metaData.h"
#include "function.h"
#include "util/threadLocal.h"
#include "memory/bufferPool.h"
namespace STORE
{
	namespace SHELL {
#define DUAL_ARGV_MATH_OP_FUNC_TYPE 0x8000000000000000ULL
#define SINGLE_ARGV_MATH_OP_FUNC_TYPE 0x4000000000000000ULL
#define DUAL_ARGV_LOGIC_OP_FUNC_TYPE 0x2000000000000000ULL
#define SINGLE_ARGV_LOGIC_OP_FUNC_TYPE 0x1000000000000000ULL
#define FUNC_ARGV_MASK 0xF000000000000000ULL
		extern bufferPool shellGlobalBufferPool;
		static constexpr int MAX_EXPRESSION_LENGTH = 1024;
		enum FieldType {
			RAW_FIELD,
			COLUMN_FIELD,
			ROW_FUNCTION_FIELD,
			GROUP_FUNCTION_FIELD,
			EXPRESSION_FIELD
		};
		struct Field {
			FieldType fieldType;
			uint8_t valueType;
			void* (*getValueFunc)(Field* field,const DATABASE_INCREASE::DMLRecord * row);
			void (*cleanFunc)(Field*);
			inline void initField(FieldType fieldType,uint8_t valueType, void* (*_getValueFunc)(Field*, const DATABASE_INCREASE::DMLRecord*) = nullptr, void (*_cleanFunc)(Field*) = nullptr)
			{
				this->fieldType = fieldType;
				this->valueType = valueType;
				getValueFunc = _getValueFunc;
				cleanFunc = _cleanFunc;
			}
			inline void* getValue(const DATABASE_INCREASE::DMLRecord* row)const
			{
				return getValueFunc((Field*)this, row);
			}
			inline void clean()
			{
				if (cleanFunc != nullptr)
					cleanFunc(this);
			}
		};
		struct varLenValue
		{
			uint32_t size;
			bool alloced;
			const char* value;
			inline void clean()
			{
				if (alloced)
					basicBufferPool::free((char*)value);
			}
		};
		struct rawField :public Field {
			void* data;
			inline void init(void* data, uint8_t type)
			{
				initField(RAW_FIELD,type, _getValueFunc, _clean);
				this->data = data;
			}
			static void* _getValueFunc( Field* field, const DATABASE_INCREASE::DMLRecord* row)
			{
				return static_cast<const rawField*>(field)->data;
			}
			static void _clean(Field* field)
			{
				if (!META::columnInfos[static_cast<rawField*>(field)->valueType].fixed)
				{
					static_cast<varLenValue*>(static_cast<rawField*>(field)->data)->clean();
					basicBufferPool::free(static_cast<rawField*>(field)->data);
				}
			}
		};

		struct columnFiled :public Field {
			const META::columnMeta* column;
			static void* _getValue(Field* field,const DATABASE_INCREASE::DMLRecord* row)
			{
				switch (field->valueType)
				{
				case META::T_INT32:
					return (void*)(uint64_t)*(int32_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_UINT32:
					return (void*)(uint64_t) * (uint32_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_INT64:
				case META::T_DATETIME:
					return (void*)(uint64_t) * (int64_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_UINT64:
				case META::T_TIMESTAMP:
					return (void*)(uint64_t) * (int64_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_FLOAT:
				{
					float v = *(float*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
					return *(void**)&v;
				}
				case META::T_DOUBLE:
				{
					float v = *(double*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
					return *(void**)&v;
				}
				case META::T_UINT16:
					return (void*)(uint64_t) * (uint16_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_INT16:
					return (void*)(uint64_t) * (int16_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_UINT8:
					return (void*)(uint64_t) * (uint8_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				case META::T_INT8:
					return (void*)(uint64_t) * (int8_t*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
				default:
				{
					if (META::columnInfos[field->valueType].fixed)
						return (void*)row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
					else
					{
						varLenValue *v  = (varLenValue*)shellGlobalBufferPool.allocByLevel(0);
						v->alloced = false;
						v->size = row->varColumnSize(static_cast<const columnFiled*>(field)->column->m_columnIndex);
						v->value = row->column(static_cast<const columnFiled*>(field)->column->m_columnIndex);
						return v;
					}
				}
				}
			}
			void init(const META::columnMeta* column)
			{
				initField(COLUMN_FIELD,column->m_columnType, _getValue);
				this->column = column;
			}
		};
		struct rowFunctionFiled :public Field
		{
			Field** argvs;
			const rowFunction* func;
			void init(Field** argvs, const rowFunction* func)
			{
				initField(ROW_FUNCTION_FIELD,func->returnValueType, _getValue, _clean);
				this->argvs = argvs;
				this->func = func;
			}
			static void* _getValue( Field* field,const DATABASE_INCREASE::DMLRecord* row)
			{
				return static_cast<const rowFunctionFiled*>(field)->func->exec(static_cast<const rowFunctionFiled*>(field)->argvs,row);
			}
			static void _clean(Field* field)
			{
				rowFunctionFiled* ff = static_cast<rowFunctionFiled*>(field);
				for (int i = 0; i < ff->func->argvCount; i++)
				{
					ff->argvs[i]->clean();
					basicBufferPool::free(ff->argvs[i]);
				}
				basicBufferPool::free(ff->argvs);
			}
		};
		struct groupFunctionFiled :public Field
		{
			Field** argvs;
			void* histroyValue;
			uint32_t rowCount;
			const groupFunction* func;
			void init(Field**  argv, const groupFunction* func)
			{
				initField(GROUP_FUNCTION_FIELD,func->returnValueType, _getValue, _clean);
				this->argvs = argv;
				this->func = func;
				histroyValue = nullptr;
				rowCount = 0;
			}
			static void* _getValue(Field* field, const DATABASE_INCREASE::DMLRecord* row)
			{
				if (row == nullptr)//finally
				{
					return static_cast<const groupFunctionFiled*>(field)->func->finalValueFunc(static_cast<groupFunctionFiled*>(field)->histroyValue, static_cast<groupFunctionFiled*>(field)->rowCount);
				}
				else
				{
					static_cast<const groupFunctionFiled*>(field)->func->exec(static_cast<const groupFunctionFiled*>(field)->argvs, static_cast<groupFunctionFiled*>(field)->histroyValue, static_cast<groupFunctionFiled*>(field)->rowCount, row);
					return nullptr;
				}
			}
			static void _clean(Field* field)
			{
				rowFunctionFiled* ff = static_cast<rowFunctionFiled*>(field);
				for (int i = 0; i < ff->func->argvCount; i++)
				{
					ff->argvs[i]->clean();
					basicBufferPool::free(ff->argvs[i]);
				}
				basicBufferPool::free(ff->argvs);
			}
		};
		struct expressionField :public Field
		{
			static threadLocal<Field**>stack;
			Field** list;
			uint16_t listSize;
			bool logicOrMath;
			bool group;
			static bool checkAndGetType(Field** list, uint16_t listSize, bool logicOrMath,uint8_t &type,bool &group);
			inline void init(Field** list, uint16_t listSize, bool logicOrMath,bool group,uint8_t type)
			{
				initField(EXPRESSION_FIELD,type, _getValue, _clean);
				this->list = list;
				this->listSize = listSize;
				this->logicOrMath = logicOrMath;
				this->group = group;
			}
			static void* _getValue( Field* field,const DATABASE_INCREASE::DMLRecord* row);
			static void _clean(Field* field);
		};
	}
}
