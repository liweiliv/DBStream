#pragma once
#if 0
namespace SQL_PARSER {
	enum class SQL_WORD_TYPE {
		STATIC_WORD,
		VAR_CHAR,
		STATIC_SIGN,
		VAR_SIGN
	};
	struct sqlWord {
		SQL_WORD_TYPE type;
		virtual bool match(const char* sqlPos, uint32_t offset, uint32_t length) const = 0;
	};
	enum class SQL_VALUE_TYPE {
		WORD,
		OPERATOR,
		INTEGER_NUMBER,
		FLOAT_NUMBER,
		SINGLE_QUOTE_WORD,
		DOUBLE_QUOTE_WORD,
		BACK_QUOTE_WORD,
		FIELD_NAME_WORD,
		FIELD_LIST,
		FUNCTION,
		LOGIC_EXPRESSION,
		VALUE_EXPRESSION
	};
	constexpr static bool SQL_TYPE_MATCH_MATRIX[12][12] = {
		{true,false,false,false,false,false,false,false,false,false,false,false},
		{false,true,false,false,false,false,false,false,false,false,false,false},
		{false,false,true,true,false,false,false,false,false,false,false,false},
		{false,false,true,true,false,false,false,false,false,false,false,false},
		{false,false,false,false,true,false,false,false,false,false,false,false},
		{false,false,false,false,false,true,false,false,false,false,false,false},
		{false,false,false,false,false,false,true,false,false,false,false,false},
		{true,false,false,false,false,false,false,true,false,false,false,false},
		{false,false,false,false,false,false,false,false,true,false,false,false},
		{false,false,false,false,false,false,false,false,false,true,false,false},
		{false,false,false,false,false,false,false,false,false,false,true,true},
		{false,false,false,false,false,false,false,false,false,false,false,true},
	};
	constexpr static bool KEY_CHAR[256] = {
	true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
	true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
	true, false, true, false, false, true, true, true, true, true, true, true, true, true, true, true,
	false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, true,
	false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
	false, false, false, false, false, false, false, false, false, false, false, true, false, false, true, false,
	true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false,
	false, false, false, false, false, false, false, false, false, false, false, true, true, true, true, false };
	struct sqlValue {
		SQL_VALUE_TYPE type;
		const char* pos;
		uint32_t length;
		sqlValue() :type(SQL_VALUE_TYPE::WORD), pos(nullptr), length(0) {}
		sqlValue(const char* s, uint32_t length) :type(SQL_VALUE_TYPE::WORD), pos(s), length(length) {}
		sqlValue(const std::string& s) :type(SQL_VALUE_TYPE::WORD), pos(s.c_str()), length(s.length()) {}
	};
	struct operationInfo;
	struct operatorSqlValue :public  sqlValue {
		const operationInfo* op;
		operatorSqlValue() :sqlValue(), op(nullptr) { type = SQL_VALUE_TYPE::OPERATOR; }
		operatorSqlValue(const char* s, uint32_t length) :sqlValue(s, length), op(nullptr) { type = SQL_VALUE_TYPE::OPERATOR; }
		operatorSqlValue(const std::string& s) :sqlValue(s), op(nullptr) { type = SQL_VALUE_TYPE::OPERATOR; }
	};
	struct fieldSqlValue :public sqlValue {
		sqlValue* first;
		sqlValue* second;
		sqlValue* third;
		uint8_t count;
		fieldSqlValue() :first(nullptr), second(nullptr), third(nullptr), count(0) { type = SQL_VALUE_TYPE::FIELD_NAME_WORD; }
		fieldSqlValue(const char* s, uint32_t length) :sqlValue(s, length), first(nullptr), second(nullptr), third(nullptr), count(0) { type = SQL_VALUE_TYPE::FIELD_NAME_WORD; }
		fieldSqlValue(const std::string& s) : sqlValue(s), first(nullptr), second(nullptr), third(nullptr), count(0) { type = SQL_VALUE_TYPE::FIELD_NAME_WORD; }
	};
	struct fieldListSqlValue :public sqlValue {
		sqlValue** list;
		uint32_t count;
		fieldListSqlValue() :list(nullptr), count(0)
		{
			type = SQL_VALUE_TYPE::FIELD_LIST;
		}
	};
	struct functionSqlValue :public sqlValue {
		fieldListSqlValue fieldList;
		functionSqlValue() :sqlValue() {
			type = SQL_VALUE_TYPE::FUNCTION;
		}
		functionSqlValue(const char* s, uint32_t length) :sqlValue(s, length) {
			type = SQL_VALUE_TYPE::FUNCTION;
		}
		functionSqlValue(const std::string& s) :sqlValue(s) {
			type = SQL_VALUE_TYPE::FUNCTION;
		}
	};

	struct expressionSqlValue :public sqlValue {
		bool logic;
		sqlValue** valueStack;
		uint16_t count;
	};
	static constexpr auto MAX_EXPRESSION_OPERATION_COUNT = 1024;
	static constexpr auto MAX_EXPRESSION_FIELD_COUNT = 1024;

	struct sql;
	struct sqlHandle {
		void* userData;
		uint32_t tid;
		bool setOrWhere;
		std::string currentDatabase;
		sql* sqlList;
		sql* tail;
		uint32_t sqlCount;
	};
	class wordCompare {
	public:
		bool caseSensitive;
		wordCompare(bool caseSensitive = false) :caseSensitive(caseSensitive) {}
		wordCompare(const wordCompare& c) :caseSensitive(c.caseSensitive) {}
		wordCompare& operator=(const wordCompare c) { caseSensitive = c.caseSensitive; return *this; }
		inline uint32_t hash(const sqlValue* v)const
		{
			uint32_t hash = 1315423911;
			const char* s = v->pos, * e = v->pos + v->length;
			if (caseSensitive)
			{
				while (s <= e)
					hash ^= ((hash << 5) + (*s++) + (hash >> 2));
			}
			else
			{
				while (s <= e)
				{
					if (*s >= 'A' && *s <= 'Z')
						hash ^= ((hash << 5) + (*s++) + ('a' - 'A') + (hash >> 2));
					else
						hash ^= ((hash << 5) + (*s++) + (hash >> 2));
				}
			}
			return (hash & 0x7FFFFFFF);
		}
		//hash
		inline uint32_t operator()(const sqlValue* word)const
		{
			return hash(word);
		}
		inline bool operator()(const sqlValue* src, const sqlValue* dest)const
		{
			if (src->length != dest->length)
				return false;
			if (caseSensitive)
				return memcmp(src->pos, dest->pos, src->length) == 0;
			else
				return strncasecmp(src->pos, dest->pos, src->length) == 0;
		}
	};
	struct stringWord :public sqlWord {
		std::string m_word;
		bool m_caseSensitive;
		sqlValue m_value;
		stringWord() :m_caseSensitive(false), m_value() {}
		stringWord(const char* word, bool caseSensitive = false) : m_word(word), m_caseSensitive(caseSensitive), m_value(m_word) {}
		stringWord(const std::string& word, bool caseSensitive = false) :m_word(word), m_caseSensitive(caseSensitive), m_value(m_word) {}
		stringWord(const stringWord& word) : m_word(word.m_word), m_caseSensitive(word.m_caseSensitive), m_value(m_word) {}
		stringWord& operator=(const stringWord& word)
		{
			type = word.type;
			m_word = word.m_word;
			m_caseSensitive = word.m_caseSensitive;
			m_value.pos = m_word.c_str();
			m_value.length = m_word.length();
			m_value.type = word.m_value.type;
			return *this;
		}
		bool operator==(const stringWord& word) const
		{
			if (m_caseSensitive != word.m_caseSensitive)
				return false;
			if (m_caseSensitive)
				return m_word.compare(word.m_word) == 0;
			else
			{
				if (m_word.length() != word.m_word.length())
					return false;
				return strncasecmp(m_word.c_str(), word.m_word.c_str(), m_word.length()) == 0;
			}
		}
		virtual bool match(const sqlValue * value) const
		{
			if (value->length != m_word.length())
				return false;
			if (m_caseSensitive)
				return strncmp(value->pos, m_word.c_str(), value->length) == 0;
			else
				return strncasecmp(value->pos, m_word.c_str(), value->length) == 0 ;
		}
		inline bool match(const char*& pos)
		{
			while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r')
				pos++;
			if ((m_caseSensitive ? strncmp(pos, m_word.c_str(), m_word.length()) : strncasecmp(pos, m_word.c_str(), m_word.length())) == 0)
			{
				if (KEY_CHAR[*(pos + m_word.length())])
				{
					pos += m_word.length();
					return true;
				}
			}
			return false;
		}
	};
}
#endif