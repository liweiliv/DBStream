#pragma once
#include "sqlParser/operationInfo.h"
#include "meta/columnType.h"
namespace STORE
{
	namespace SHELL {
		typedef void* (*DualArgvMathOperater)(void*, void*);
		typedef void* (*SingleArgvMathOperater)(void*);
		typedef bool (*DualArgvLogicOperater)(void*, void*);
		typedef bool (*SingleArgvLogicOperater)(void*);

		struct operatorFuncInfo {
			uint8_t returnType;
			void* func;
			operatorFuncInfo(uint8_t returnType, void* func) :returnType(returnType), func(func) {}
		};
		struct dualArgvMathOperatorMatrix
		{
			SQL_PARSER::OPERATOR op;
			operatorFuncInfo*** matrix;
			dualArgvMathOperatorMatrix(SQL_PARSER::OPERATOR op) :op(op), matrix(nullptr) {}
			void set(uint8_t returnType, DualArgvMathOperater func, uint8_t ltype, uint8_t rtype)
			{
				if (matrix == nullptr)
				{
					matrix = new operatorFuncInfo * *[META::T_CURRENT_VERSION_MAX_TYPE];
					memset(matrix, 0, sizeof(operatorFuncInfo * *) * META::T_CURRENT_VERSION_MAX_TYPE);
				}
				if (matrix[ltype] == nullptr)
				{
					matrix[ltype] = new operatorFuncInfo * [META::T_CURRENT_VERSION_MAX_TYPE];
					memset(matrix[ltype], 0, sizeof(operatorFuncInfo*) * META::T_CURRENT_VERSION_MAX_TYPE);
				}
				if (matrix[ltype][rtype] == nullptr)
					matrix[ltype][rtype] = new operatorFuncInfo(returnType, func);
			}
			inline operatorFuncInfo* get(uint8_t ltype, uint8_t rtype)
			{
				if (matrix == nullptr || matrix[ltype] == nullptr || matrix[ltype][rtype] == nullptr)
					return nullptr;
				return matrix[ltype][rtype];
			}
			~dualArgvMathOperatorMatrix()
			{
				if (matrix == nullptr)
					return;
				for (int i = 0; i < META::T_CURRENT_VERSION_MAX_TYPE; i++)
				{
					if (matrix[i] == nullptr)
						continue;
					for (int j = 0; j < META::T_CURRENT_VERSION_MAX_TYPE; j++)
					{
						if (matrix[i][j] != nullptr)
							delete matrix[i][j];
					}
					delete[]matrix[i];
				}
				delete[] matrix;
			}
		};
		struct singleArgvMathOperatorMatrix
		{
			SQL_PARSER::OPERATOR op;
			operatorFuncInfo** matrix;
			singleArgvMathOperatorMatrix(SQL_PARSER::OPERATOR op) :op(op), matrix(nullptr) {}
			void set(uint8_t returnType, SingleArgvMathOperater func, uint8_t type)
			{
				if (matrix == nullptr)
				{
					matrix = new operatorFuncInfo *[META::T_CURRENT_VERSION_MAX_TYPE];
					memset(matrix, 0, sizeof(operatorFuncInfo *) * META::T_CURRENT_VERSION_MAX_TYPE);
				}
				matrix[type] = new operatorFuncInfo(returnType, func);
			}
			inline operatorFuncInfo* get(uint8_t type)
			{
				if (matrix == nullptr || matrix[type] == nullptr)
					return nullptr;
				return matrix[type];
			}
			~singleArgvMathOperatorMatrix()
			{
				if (matrix == nullptr)
					return;
				for (int i = 0; i < META::T_CURRENT_VERSION_MAX_TYPE; i++)
				{
					if (matrix[i] != nullptr)
						delete matrix[i];
				}
				delete[] matrix;
			}
		};
		struct dualArgvLogicOperatorMatrix
		{
			SQL_PARSER::OPERATOR op;
			operatorFuncInfo*** matrix;
			dualArgvLogicOperatorMatrix(SQL_PARSER::OPERATOR op) :op(op), matrix(nullptr) {}
			void set(DualArgvLogicOperater func, uint8_t ltype, uint8_t rtype)
			{
				if (matrix == nullptr)
				{
					matrix = new operatorFuncInfo * *[META::T_CURRENT_VERSION_MAX_TYPE];
					memset(matrix, 0, sizeof(operatorFuncInfo * *) * META::T_CURRENT_VERSION_MAX_TYPE);
				}
				if (matrix[ltype] == nullptr)
				{
					matrix[ltype] = new operatorFuncInfo * [META::T_CURRENT_VERSION_MAX_TYPE];
					memset(matrix[ltype], 0, sizeof(operatorFuncInfo*) * META::T_CURRENT_VERSION_MAX_TYPE);
				}
				if (matrix[ltype][rtype] == nullptr)
					matrix[ltype][rtype] = new operatorFuncInfo(META::T_BOOL, func);
			}
			inline operatorFuncInfo* get(uint8_t ltype, uint8_t rtype)
			{
				if (matrix == nullptr || matrix[ltype] == nullptr || matrix[ltype][rtype] == nullptr)
					return nullptr;
				return matrix[ltype][rtype];
			}
			~dualArgvLogicOperatorMatrix()
			{
				if (matrix == nullptr)
					return;
				for (int i = 0; i < META::T_CURRENT_VERSION_MAX_TYPE; i++)
				{
					if (matrix[i] == nullptr)
						continue;
					for (int j = 0; j < META::T_CURRENT_VERSION_MAX_TYPE; j++)
					{
						if (matrix[i][j] != nullptr)
							delete matrix[i][j];
					}
					delete[]matrix[i];
				}
				delete[] matrix;
			}
		};
		struct singleArgvLogicOperatorMatrix
		{
			SQL_PARSER::OPERATOR op;
			operatorFuncInfo** matrix;
			singleArgvLogicOperatorMatrix(SQL_PARSER::OPERATOR op) :op(op), matrix(nullptr) {}
			void set(SingleArgvLogicOperater func, uint8_t type)
			{
				if (matrix == nullptr)
				{
					matrix = new operatorFuncInfo * [META::T_CURRENT_VERSION_MAX_TYPE];
					memset(matrix, 0, sizeof(operatorFuncInfo*) * META::T_CURRENT_VERSION_MAX_TYPE);
				}
				matrix[type] = new operatorFuncInfo(META::T_BOOL, func);
			}
			inline operatorFuncInfo* get(uint8_t type)
			{
				if (matrix == nullptr || matrix[type] == nullptr)
					return nullptr;
				return matrix[type];
			}
			~singleArgvLogicOperatorMatrix()
			{
				if (matrix == nullptr)
					return;
				for (int i = 0; i < META::T_CURRENT_VERSION_MAX_TYPE; i++)
				{
					if (matrix[i] != nullptr)
						delete matrix[i];
				}
				delete[] matrix;
			}
		};
		extern  dualArgvMathOperatorMatrix* dualArgvMathOperators[SQL_PARSER::NOT_OPERATION];
		static inline operatorFuncInfo* getDualArgvMathFunc(SQL_PARSER::OPERATOR op, uint8_t ltype, uint8_t rtype)
		{
			if (dualArgvMathOperators[op] == nullptr)
				return nullptr;
			return dualArgvMathOperators[op]->get(ltype,rtype);
		}
		extern  singleArgvMathOperatorMatrix* singleArgvMathOperators[SQL_PARSER::NOT_OPERATION];
		static inline operatorFuncInfo* getSingleArgvMathFunc(SQL_PARSER::OPERATOR op, uint8_t type)
		{
			if (singleArgvMathOperators[op] == nullptr)
				return nullptr;
			return singleArgvMathOperators[op]->get(type);
		}
		extern  dualArgvLogicOperatorMatrix* dualArgvLogicOperators[SQL_PARSER::NOT_OPERATION];
		static inline operatorFuncInfo* getDualArgvLogicFunc(SQL_PARSER::OPERATOR op, uint8_t ltype, uint8_t rtype)
		{
			if (dualArgvLogicOperators[op] == nullptr)
				return nullptr;
			return dualArgvLogicOperators[op]->get(ltype, rtype);
		}
		extern  singleArgvLogicOperatorMatrix* singleArgvLogicOperators[SQL_PARSER::NOT_OPERATION];
		static inline operatorFuncInfo* getSingleArgvLogicFunc(SQL_PARSER::OPERATOR op, uint8_t type)
		{
			if (singleArgvLogicOperators[op] == nullptr)
				return nullptr;
			return singleArgvLogicOperators[op]->get(type);
		}
	}
}