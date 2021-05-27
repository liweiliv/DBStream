#pragma once
#include <list>
#include <glog/logging.h>
#include "util/sparsepp/spp.h"
#include "util/unorderMapUtil.h"
#include "util/status.h"
#include "util/json.h"

namespace DATA_SOURCE
{
	constexpr static auto HAS_SHARDING_RULE = "hasShardingRule";
	constexpr static auto SCHEMA = "schema";
	constexpr static auto TABLE = "table";
	constexpr static auto TRANSLATE_TO = "translateTo";
	constexpr static auto NEED_ALL_TABLE = "needAllTable";
	class shardingRuleFormat
	{
	private:
		enum class fragmentType {
			STR,
			SHARDING_ID
		};
		struct fragment
		{
			fragmentType type;
			virtual bool match(const char*& str) = 0;
			fragment(fragmentType t) :type(t) {}
			virtual ~fragment(){}
		};
		struct strFragment : public fragment
		{
			std::string string;
			strFragment() :fragment(fragmentType::STR) {}
			virtual bool match(const char*& str)
			{
				if (strncmp(string.c_str(), str, string.length()) == 0)
				{
					str += string.length();
					return true;
				}
				return false;
			}
		};
		struct shardingIdFragment : public fragment
		{
			int fixedSize;
			shardingIdFragment() :fragment(fragmentType::SHARDING_ID), fixedSize(0) {}
			virtual bool match(const char*& str)
			{
				if (fixedSize > 0)
				{
					for (int i = 0; i < fixedSize; i++)
					{
						if (str[i] > '9' || str[i] < '0')
							return false;
					}
					str += fixedSize;
					return true;
				}
				else
				{
					if (str[0] > '9' || str[0] < '0')
						return false;
					str++;
					while (str[0] <= '9' && str[0] >= '0')
						str++;
					return true;
				}
			}
		};
		fragment** fragments;
		uint32_t count;
		static void getFragmentCount(const char* rule, int& shardingRuleFragCount, int& strFragCount)
		{
			shardingRuleFragCount = 0;
			strFragCount = 0;
			const char* pos = rule;
			while (*pos != '\0')
			{
				const char* start = pos;
				do {
					while (*pos != '\0' && *pos != '%')
						pos++;
					if (*pos == '\0')
					{
						if (pos != start)
							strFragCount++;
						break;
					}
					const char* tmp = pos++;
					while (pos[0] <= '9' && pos[0] >= '0')
						pos++;
					if (*pos == 'd')
					{
						if (tmp != start)
							strFragCount++;
						shardingRuleFragCount++;
						pos++;
						break;
					}
					pos++;
				} while (true);
			}
		}
	public:
		shardingRuleFormat(const char* rule): fragments(nullptr), count(0)
		{
			const char* pos = rule;
			int shardingRuleFragCount = 0, strFragCount = 0;
			getFragmentCount(rule, shardingRuleFragCount, strFragCount);

			fragments = new  fragment * [count = shardingRuleFragCount + strFragCount];
			int fid = 0;
			pos = rule;
			while (*pos != '\0')
			{
				const char* start = pos;
				do {
					while (*pos != '\0' && *pos != '%')
						pos++;
					if (*pos == '\0')
					{
						if (pos != start)
						{
							strFragment* sf = new strFragment();
							sf->string.assign(start, pos - start);
							fragments[fid++] = sf;
						}
						break;
					}
					const char* tmp = pos++;
					while (pos[0] <= '9' && pos[0] >= '0')
						pos++;
					if (*pos == 'd')
					{
						if (tmp != start)
						{
							strFragment* sf = new strFragment();
							sf->string.assign(start, tmp - start);
							fragments[fid++] = sf;
						}
						shardingIdFragment* sif = new  shardingIdFragment();
						if (pos - tmp > 1)
						{
							for (int i = 1; i < pos - tmp; i++)
								sif->fixedSize = sif->fixedSize * 10 + tmp[i] - '0';
						}
						fragments[fid++] = sif;
						pos++;
						break;
					}
					pos++;
				} while (true);
			}
		}
		~shardingRuleFormat()
		{
			if (fragments != nullptr)
			{
				for (uint32_t i = 0; i < count; i++)
				{
					if (fragments[i] != nullptr)
						delete fragments[i];
				}
				delete[]fragments;
			}
		}
		bool match(const char* str) const
		{
			const char* pos = str;
			for (uint32_t i = 0; i < count; i++)
			{
				if (!fragments[i]->match(pos))
					return false;
			}
			return true;
		}
		static bool hasShardingRule(const char* rule)
		{
			int shardingRuleFragCount = 0, strFragCount = 0;
			getFragmentCount(rule, shardingRuleFragCount, strFragCount);
			return shardingRuleFragCount > 0;
		}
		static DS loadFormat(const char * name, const jsonObject * json, shardingRuleFormat *& format)
		{
			format = nullptr;
			const jsonValue* j = json->get(HAS_SHARDING_RULE);
			if (j != nullptr)
			{
				if (j->t != JSON_TYPE::J_BOOL)
					dsFailedAndLogIt(1, HAS_SHARDING_RULE << " info for tableList is not null and must be json bool", ERROR);
				if (static_cast<const jsonBool*>(j)->m_value && shardingRuleFormat::hasShardingRule(name))
					format = new shardingRuleFormat(name);
			}
			dsOk();
		}

	};
	class tableList
	{
	private:
		struct tableInfo
		{
			std::string name;
			std::string translateTo;
			DS load(const jsonObject* json)
			{
				for (jsonObjectMap::const_iterator iter = json->m_values.begin(); iter != json->m_values.end(); iter++)
				{
					if (iter->first.compare(TRANSLATE_TO) == 0)
					{
						if (iter->second->t != JSON_TYPE::J_STRING)
							dsFailedAndLogIt(1, "type of " << name << "." << TRANSLATE_TO << " must be json string", ERROR);
						translateTo = static_cast<const jsonString*>(iter->second)->m_value;
					}
					else if (iter->first.compare(HAS_SHARDING_RULE) != 0)
						dsFailedAndLogIt(1, "table:" << name << " has unsupport attr " << iter->first, ERROR);
				}
				dsOk();
			}
		};
		typedef spp::sparse_hash_map<const char*, tableInfo*, StrHash, StrCompare> tableHashMap;
		struct schemaInfo
		{
			std::string name;
			std::string translateTo;
			bool needAllTable;
			bool useShardingRule;
			tableHashMap tables;
			std::list<std::pair<shardingRuleFormat*, tableInfo*>> useRuleTableList;
			void clear()
			{
				for (tableHashMap::iterator iter = tables.begin(); iter != tables.end(); iter++)
					delete iter->second;
				for (std::list<std::pair<shardingRuleFormat*, tableInfo*>>::iterator riter = useRuleTableList.begin(); riter != useRuleTableList.end(); riter++)
				{
					if (riter->first != nullptr)
						delete riter->first;
					if (riter->second != nullptr)
						delete riter->second;
				}
				tables.clear();
				useRuleTableList.clear();
			}
			~schemaInfo()
			{
				clear();
			}
			bool containTable(const char* tableName) const 
			{
				if (needAllTable)
					return true;
				if (tables.find(tableName) != tables.end())
					return true;
				for (std::list<std::pair<shardingRuleFormat*, tableInfo*>>::const_iterator riter = useRuleTableList.begin(); riter != useRuleTableList.end(); riter++)
				{
					if (riter->first->match(tableName))
						return true;
				}
				return false;
			}

			DS loadAttr(jsonObjectMap::const_iterator & iter)
			{
				if (iter->first.compare(TABLE) == 0)
				{
					if (iter->second->t != JSON_TYPE::J_OBJECT)
						dsFailedAndLogIt(1, "type of " << name << "." << iter->first << " must be json object", ERROR);
					const jsonObject* jTables = static_cast<const jsonObject*>(iter->second);
					for (jsonObjectMap::const_iterator titer = jTables->m_values.begin(); titer != jTables->m_values.end(); titer++)
					{
						if (titer->second->t != JSON_TYPE::J_OBJECT)
							dsFailedAndLogIt(1, "type of " << name << "." << titer->first << " must be json object", ERROR);
						shardingRuleFormat* format = nullptr;
						dsReturnIfFailedWithOp(shardingRuleFormat::loadFormat(titer->first.c_str(), static_cast<const jsonObject*>(titer->second), format), clear());
						tableInfo* table = new tableInfo();
						table->name = titer->first;
						if (format != nullptr)
							useRuleTableList.push_back(std::pair<shardingRuleFormat*, tableInfo*>(format, table));
						else
							tables.insert(std::pair<const char*, tableInfo*>(table->name.c_str(), table));
						dsReturnIfFailedWithOp(table->load(static_cast<const jsonObject*>(titer->second)),clear());
					}
				}
				else if (iter->first.compare(TRANSLATE_TO) == 0)
				{
					if (iter->second->t != JSON_TYPE::J_STRING)
						dsFailedAndLogIt(1, "type of " << name << "." << TRANSLATE_TO << " must be json string", ERROR);
					translateTo = static_cast<const jsonString*>(iter->second)->m_value;
				}
				else if (iter->first.compare(NEED_ALL_TABLE) == 0)
				{
					if (iter->second->t != JSON_TYPE::J_BOOL)
						dsFailedAndLogIt(1, "type of " << name << "." << NEED_ALL_TABLE << " must be json bool", ERROR);
					needAllTable = static_cast<const jsonBool*>(iter->second)->m_value;
				}
				else if (iter->first.compare(HAS_SHARDING_RULE) != 0)
					dsFailedAndLogIt(1, "db:" << name << " has unsupport attr " << iter->first, ERROR);
				dsOk();
			}

			DS load(const jsonObject* json)
			{
				for (jsonObjectMap::const_iterator iter = json->m_values.begin(); iter != json->m_values.end(); iter++)
					dsReturnIfFailed(loadAttr(iter));
				dsOk();
			}
		};
		typedef spp::sparse_hash_map<const char*, schemaInfo*, StrHash, StrCompare> schemaHashMap;
		struct dbInfo :public schemaInfo
		{
			schemaHashMap schemas;
			std::list<std::pair<shardingRuleFormat*, schemaInfo*>> useRuleSchemaList;
			schemaInfo* getSchema(const char* schemaName) const 
			{
				schemaHashMap::const_iterator iter = schemas.find(schemaName);
				if (iter == schemas.end())
				{
					for (std::list<std::pair<shardingRuleFormat*, schemaInfo*>>::const_iterator riter = useRuleSchemaList.begin(); riter != useRuleSchemaList.end(); riter++)
					{
						if (riter->first->match(schemaName))
							return riter->second;
					}
					return nullptr;
				}
				return iter->second;
			}
			void clear()
			{
				schemaInfo::clear();
				for (schemaHashMap::iterator iter = schemas.begin(); iter != schemas.end(); iter++)
					delete iter->second;
				for (std::list<std::pair<shardingRuleFormat*, schemaInfo*>>::iterator riter = useRuleSchemaList.begin(); riter != useRuleSchemaList.end(); riter++)
				{
					if (riter->first != nullptr)
						delete riter->first;
					if (riter->second != nullptr)
						delete riter->second;
				}

				schemas.clear();
				useRuleSchemaList.clear();
			}
			bool contain(const char* schemaName, const char* tableName) const 
			{
				if (needAllTable)
					return true;
				schemaInfo* schema = getSchema(schemaName);
				if (schema == nullptr)
					return false;
				return schema->containTable(tableName);
			}
			DS loadAttr(jsonObjectMap::const_iterator& iter)
			{
				if (iter->first.compare(SCHEMA) == 0)
				{
					if (iter->second->t != JSON_TYPE::J_OBJECT)
						dsFailedAndLogIt(1, "type of " << name << "." << iter->first << " must be json object", ERROR);
					const jsonObject* jSchemas = static_cast<const jsonObject*>(iter->second);
					for (jsonObjectMap::const_iterator siter = jSchemas->m_values.begin(); siter != jSchemas->m_values.end(); siter++)
					{
						if (siter->second->t != JSON_TYPE::J_OBJECT)
							dsFailedAndLogIt(1, "type of " << name << "." << siter->first << " must be json object", ERROR);
						shardingRuleFormat* format = nullptr;
						dsReturnIfFailedWithOp(shardingRuleFormat::loadFormat(siter->first.c_str(), static_cast<const jsonObject*>(siter->second), format), clear());
						schemaInfo* scheam = new schemaInfo();
						scheam->name = siter->first;
						if (format != nullptr)
							useRuleSchemaList.push_back(std::pair<shardingRuleFormat*, schemaInfo*>(format, scheam));
						else
							schemas.insert(std::pair<const char*, schemaInfo*>(scheam->name.c_str(), scheam));
						dsReturnIfFailedWithOp(scheam->load(static_cast<const jsonObject*>(siter->second)), clear());
					}
				}
				else
				{
					dsReturn(schemaInfo::loadAttr(iter));
				}
				dsOk();
			}
			DS load(jsonObject* json)
			{
				if (json->get(SCHEMA) != nullptr && json->get(TABLE) != nullptr)
					dsFailedAndLogIt(1, "db:" << name << " can not have both " << SCHEMA << " and " << TABLE, ERROR);
				for (jsonObjectMap::const_iterator iter = json->m_values.begin(); iter != json->m_values.end(); iter++)
					dsReturnIfFailed(loadAttr(iter));
				dsOk();
			}
		};
		typedef spp::sparse_hash_map<const char*, dbInfo*, StrHash, StrCompare> DBHashMap;
		DBHashMap m_dbList;
		std::list<std::pair<shardingRuleFormat*, dbInfo*>> m_useRuleDbList;
		bool m_needAllTables;
	private:
		const dbInfo* matchDb(const char* dbName) const 
		{
			DBHashMap::const_iterator iter = m_dbList.find(dbName);
			if (iter != m_dbList.end())
				return iter->second;
			if (!m_useRuleDbList.empty())
			{
				for (std::list<std::pair<shardingRuleFormat*, dbInfo*>>::const_iterator riter = m_useRuleDbList.begin(); riter != m_useRuleDbList.end(); riter++)
				{
					if (riter->first->match(dbName))
						return riter->second;
				}
			}
			return nullptr;
		}
	public:
		tableList() :m_needAllTables(false)
		{
		}
		~tableList()
		{
			clear();
		}
		void clear()
		{
			for (DBHashMap::iterator iter = m_dbList.begin(); iter != m_dbList.end(); iter++)
				delete iter->second;
			for (std::list<std::pair<shardingRuleFormat*, dbInfo*>>::iterator riter = m_useRuleDbList.begin(); riter != m_useRuleDbList.end(); riter++)
			{
				if (riter->first != nullptr)
					delete riter->first;
				if (riter->second != nullptr)
					delete riter->second;
			}
			m_dbList.clear();
			m_useRuleDbList.clear();
		}
		bool contain(const char* dbName) const
		{
			if (m_needAllTables)
				return true;
			return matchDb(dbName) != nullptr;
		}
		bool contain(const char* dbName, const char* tableName) const
		{
			if (m_needAllTables)
				return true;
			const dbInfo* db = matchDb(dbName);
			if (db == nullptr)
				return false;
			return db->containTable(tableName);
		}
		bool contain(const char* dbName, const char* schemaname, const char* tableName) const //for PostgreSQL
		{
			if (m_needAllTables)
				return true;
			const dbInfo* db = matchDb(dbName);
			if (db == nullptr)
				return false;
			return db->contain(schemaname, tableName);
		}
		DS init(const char* jsonRule)
		{
			jsonValue* json;
			int size = strlen(jsonRule);
			dsReturnIfFailed(jsonValue::parse(json, jsonRule, size));
			if (json->t != JSON_TYPE::J_OBJECT)
				dsFailedAndLogIt(1, "jsonRule for tableList must be json object", ERROR);
			jsonObject* dbList = static_cast<jsonObject*>(json);
			for (jsonObjectMap::iterator iter = dbList->m_values.begin(); iter != dbList->m_values.end(); iter++)
			{
				if (iter->second == nullptr || iter->second->t != JSON_TYPE::J_OBJECT)
				{
					clear();
					dsFailedAndLogIt(1, "db info for tableList is not null and must be json object", ERROR);
				}

				jsonObject* childs = static_cast<jsonObject*>(iter->second);
				shardingRuleFormat* format = nullptr;
				dsReturnIfFailedWithOp(shardingRuleFormat::loadFormat(iter->first.c_str(), childs, format), clear());
				dbInfo* db = new dbInfo();
				db->name.assign(iter->first);
				if (format != nullptr)
					m_useRuleDbList.push_back(std::pair<shardingRuleFormat*, dbInfo*>(new shardingRuleFormat(iter->first.c_str()), db));
				else
					m_dbList.insert(std::pair<const char*, dbInfo*>(db->name.c_str(), db));
				dsReturnIfFailedWithOp(db->load(childs), clear());
			}
			dsOk();
		}
	};
}
