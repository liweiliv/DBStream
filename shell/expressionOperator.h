#pragma once
#include "sqlParser/operationInfo.h"
#include "meta/columnType.h"

namespace SHELL {
	typedef void* (*DualArgvMathOperater)(void*, void*);
	typedef void* (*SingleArgvMathOperater)(void*);
	typedef bool (*DualArgvLogicOperater)(void*, void*);
	typedef bool (*SingleArgvLogicOperater)(void*);

	struct operatorFuncInfo {
		META::COLUMN_TYPE returnType;
		void* func;
		operatorFuncInfo(META::COLUMN_TYPE returnType, void* func) :returnType(returnType), func(func) {}
	};
	struct dualArgvMathOperatorMatrix
	{
		SQL_PARSER::OPERATOR op;
		operatorFuncInfo*** matrix;
		dualArgvMathOperatorMatrix(SQL_PARSER::OPERATOR op) :op(op), matrix(nullptr) {}
		void set(META::COLUMN_TYPE returnType, DualArgvMathOperater func, META::COLUMN_TYPE ltype, META::COLUMN_TYPE rtype)
		{
			if (matrix == nullptr)
			{
				matrix = new operatorFuncInfo * *[static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE)];
				memset(matrix, 0, sizeof(operatorFuncInfo**) * static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE));
			}
			if (matrix[TID(ltype)] == nullptr)
			{
				matrix[TID(ltype)] = new operatorFuncInfo * [static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE)];
				memset(matrix[TID(ltype)], 0, sizeof(operatorFuncInfo*) * static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE));
			}
			if (matrix[TID(ltype)][TID(rtype)] == nullptr)
				matrix[TID(ltype)][TID(rtype)] = new operatorFuncInfo(returnType, (void*)func);
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
			for (uint8_t i = 0; i < static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE); i++)
			{
				if (matrix[i] == nullptr)
					continue;
				for (uint8_t j = 0; j < static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE); j++)
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
		void set(META::COLUMN_TYPE returnType, SingleArgvMathOperater func, META::COLUMN_TYPE type)
		{
			if (matrix == nullptr)
			{
				matrix = new operatorFuncInfo * [static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE)];
				memset(matrix, 0, sizeof(operatorFuncInfo*) * static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE));
			}
			matrix[TID(type)] = new operatorFuncInfo(returnType, (void*)func);
		}
		inline operatorFuncInfo* get(META::COLUMN_TYPE type)
		{
			if (matrix == nullptr || matrix[TID(type)] == nullptr)
				return nullptr;
			return matrix[TID(type)];
		}
		~singleArgvMathOperatorMatrix()
		{
			if (matrix == nullptr)
				return;
			for (uint8_t i = 0; i < static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE); i++)
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
		void set(DualArgvLogicOperater func, META::COLUMN_TYPE ltype, META::COLUMN_TYPE rtype)
		{
			if (matrix == nullptr)
			{
				matrix = new operatorFuncInfo * *[static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE)];
				memset(matrix, 0, sizeof(operatorFuncInfo**) * static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE));
			}
			if (matrix[static_cast<uint8_t>(ltype)] == nullptr)
			{
				matrix[static_cast<uint8_t>(ltype)] = new operatorFuncInfo * [static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE)];
				memset(matrix[static_cast<uint8_t>(ltype)], 0, sizeof(operatorFuncInfo*) * static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE));
			}
			if (matrix[static_cast<uint8_t>(ltype)][static_cast<uint8_t>(rtype)] == nullptr)
				matrix[static_cast<uint8_t>(ltype)][static_cast<uint8_t>(rtype)] = new operatorFuncInfo(META::COLUMN_TYPE::T_BOOL, (void*)func);
		}
		inline operatorFuncInfo* get(META::COLUMN_TYPE ltype, META::COLUMN_TYPE rtype)
		{
			if (matrix == nullptr || matrix[TID(ltype)] == nullptr || matrix[TID(ltype)][TID(rtype)] == nullptr)
				return nullptr;
			return matrix[TID(ltype)][TID(rtype)];
		}
		~dualArgvLogicOperatorMatrix()
		{
			if (matrix == nullptr)
				return;
			for (int i = 0; i < static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE); i++)
			{
				if (matrix[i] == nullptr)
					continue;
				for (int j = 0; j < static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE); j++)
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
				matrix = new operatorFuncInfo * [static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE)];
				memset(matrix, 0, sizeof(operatorFuncInfo*) * static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE));
			}
			matrix[type] = new operatorFuncInfo(META::COLUMN_TYPE::T_BOOL, (void*)func);
		}
		inline operatorFuncInfo* get(META::COLUMN_TYPE type)
		{
			if (matrix == nullptr || matrix[TID(type)] == nullptr)
				return nullptr;
			return matrix[TID(type)];
		}
		~singleArgvLogicOperatorMatrix()
		{
			if (matrix == nullptr)
				return;
			for (uint8_t i = 0; i < static_cast<uint8_t>(META::COLUMN_TYPE::T_MAX_TYPE); i++)
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
		return dualArgvMathOperators[op]->get(ltype, rtype);
	}
	extern  singleArgvMathOperatorMatrix* singleArgvMathOperators[SQL_PARSER::NOT_OPERATION];
	static inline operatorFuncInfo* getSingleArgvMathFunc(SQL_PARSER::OPERATOR op, META::COLUMN_TYPE type)
	{
		if (singleArgvMathOperators[op] == nullptr)
			return nullptr;
		return singleArgvMathOperators[op]->get(type);
	}
	extern  dualArgvLogicOperatorMatrix* dualArgvLogicOperators[SQL_PARSER::NOT_OPERATION];
	static inline operatorFuncInfo* getDualArgvLogicFunc(SQL_PARSER::OPERATOR op, META::COLUMN_TYPE ltype, META::COLUMN_TYPE rtype)
	{
		if (dualArgvLogicOperators[op] == nullptr)
			return nullptr;
		return dualArgvLogicOperators[op]->get(ltype, rtype);
	}
	extern  singleArgvLogicOperatorMatrix* singleArgvLogicOperators[SQL_PARSER::NOT_OPERATION];
	static inline operatorFuncInfo* getSingleArgvLogicFunc(SQL_PARSER::OPERATOR op, META::COLUMN_TYPE type)
	{
		if (singleArgvLogicOperators[op] == nullptr)
			return nullptr;
		return singleArgvLogicOperators[op]->get(type);
	}
	void initExpressionOperators();
}

