/*
 * appendingIndex.cpp
 *
 *  Created on: 2019年3月12日
 *      Author: liwei
 */
#include "appendingIndex.h"
namespace STORE{
    binaryType::binaryType():data(nullptr),size(0){

    }
    binaryType::binaryType(const char* _data,uint16_t _size):data(_data),size(_size){

    }
    binaryType::binaryType(const binaryType & dest):data(dest.data),size(dest.size)
    {
    }
    binaryType binaryType::operator=(const binaryType & dest)
    {
        data = dest.data;
		size = dest.size;
        return *this;
    }
    int binaryType::compare (const binaryType & dest) const
    {
        if(size == dest.size)
            return memcmp(data,dest.data,size);
        else if(size>dest.size)
        {
            if(memcmp(data,dest.data,size)>=0)
                return -1;
            else
                return 1;
        }
        else
        {
            if(memcmp(data,dest.data,dest.size)>0)
                return 1;
            else
                return -1;
        }
    }
    bool binaryType::operator< (const binaryType & dest) const
    {
        return  compare(dest)<0;
    }
    bool binaryType::operator> (const binaryType & dest) const
    {
        return  compare(dest)>0;
    }
	unionKey::unionKey() :key(nullptr), meta(nullptr) {}
	unionKey::unionKey(const unionKey & dest) :key(dest.key), meta(dest.meta)
	{
	}
	int unionKey::compare(const unionKey & dest) const
	{
		assert(meta == dest.meta);
		const char * srcKey = key, *destKey = dest.key;
		for (uint16_t i = 0; i < meta->m_keyCount; i++)
		{
			switch (meta->m_types[i])
			{
			case UINT8:
				if (*(uint8_t*)srcKey != *(uint8_t*)destKey)
					return *(uint8_t*)srcKey - *(uint8_t*)destKey;
				break;
			case INT8:
				if (*(int8_t*)srcKey != *(int8_t*)destKey)
					return *(int8_t*)srcKey - *(int8_t*)destKey;
				break;
			case UINT16:
				if (*(uint16_t*)srcKey != *(uint16_t*)destKey)
					return *(uint16_t*)srcKey - *(uint16_t*)destKey;
				break;
			case INT16:
				if (*(int16_t*)srcKey != *(int16_t*)destKey)
					return *(int16_t*)srcKey - *(int16_t*)destKey;
				break;
			case UINT32:
				if (*(uint32_t*)srcKey != *(uint32_t*)destKey)
					return *(uint32_t*)srcKey - *(uint32_t*)destKey;
				break;
			case INT32:
				if (*(int32_t*)srcKey != *(int32_t*)destKey)
					return *(int32_t*)srcKey - *(int32_t*)destKey;
				break;
			case UINT64:
				if (*(uint64_t*)srcKey != *(uint64_t*)destKey)
					if (*(uint64_t*)srcKey > *(uint64_t*)destKey)
						return 1;
					else
						return -1;
				break;
			case INT64:
				if (*(int64_t*)srcKey != *(int64_t*)destKey)
					if (*(int64_t*)srcKey > *(int64_t*)destKey)
						return 1;
					else
						return -1;
				break;
			case FLOAT:
				if (*(float*)srcKey - *(float*)destKey > 0.000001f || *(float*)srcKey - *(float*)destKey < -0.000001f)
					if (*(float*)srcKey > *(float*)destKey)
						return 1;
					else
						return -1;
				break;
			case DOUBLE:
				if (*(double*)srcKey - *(double*)destKey > 0.000001f || *(double*)srcKey - *(double*)destKey < -0.000001f)
					if (*(double*)srcKey > *(double*)destKey)
						return 1;
					else
						return -1;
				break;
			case STRING:
			case BLOB:
			{
				binaryType s(srcKey+sizeof(uint16_t),*(uint16_t*)srcKey), d(destKey + sizeof(uint16_t), *(uint16_t*)destKey);
				int c = s.compare(d);
				if (c != 0)
					return c;
				srcKey += sizeof(uint16_t) + *(uint16_t*)srcKey;
				destKey += sizeof(uint16_t) + *(uint16_t*)destKey;
				break;
			}
			default:
				abort();
			}
			srcKey += columnInfos[meta->m_types[i]].columnTypeSize;
			destKey += columnInfos[meta->m_types[i]].columnTypeSize;
		}
		return 0;
	}
	bool unionKey::operator> (const unionKey & dest) const
	{
		return compare(dest) > 0;
	}
	/*
	*format :
	fixed:[column1][column2]...[column n]
	var:[16bit length][if(fixed) column 1][if(var) column 2 value offset in extern data]...[column n]
	*/
	const char* unionKey::initKey(leveldb::Arena * arena, unionKeyMeta * keyMeta, uint16_t *columnIdxs, uint16_t columnCount,const DATABASE_INCREASE::DMLRecord * r, bool keyUpdated)
	{
		uint32_t externSize = 0;
		char *  key = nullptr;
		char *externPtr = nullptr;
		if (!keyMeta->m_fixed)
		{
			for (uint16_t idx = 0; idx < keyMeta->m_keyCount; idx++)
			{
				if (!columnInfos[keyMeta->m_types[idx]].fixed)
				{
					if (keyUpdated)
						externSize += r->oldVarColumnSizeOfUpdateType(columnIdxs[idx], r->oldColumnOfUpdateType(columnIdxs[idx])) + sizeof(uint16_t);
					else
						externSize += r->varColumnSize(columnIdxs[idx]);
				}
			}
			key = arena->Allocate(keyMeta->m_size + externSize + sizeof(uint16_t));
			externPtr = key + keyMeta->m_size;
			*(uint16_t*)externPtr = keyMeta->m_size + externSize;
			externPtr += sizeof(uint16_t);
		}
		else
			key = arena->Allocate(keyMeta->m_size);
		char * ptr = key;
		if (r->head->type == DATABASE_INCREASE::R_INSERT|| r->head->type == DATABASE_INCREASE::R_DELETE ||
			((r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE))&&!keyUpdated)
		{
			for (uint16_t i = 0; i < keyMeta->m_keyCount; i++)
			{
				if (columnInfos[keyMeta->m_types[i]].fixed)
				{
					memcpy(ptr, r->column(columnIdxs[i]), columnInfos[keyMeta->m_types[i]].columnTypeSize);
					ptr += columnInfos[keyMeta->m_types[i]].columnTypeSize;
				}
				else
				{
					*(uint16_t*)ptr = externPtr - key;
					*(uint16_t*)externPtr = r->varColumnSize(i);
					memcpy(externPtr+sizeof(uint16_t), r->column(columnIdxs[i]), *(uint16_t*)externPtr);
					externPtr += *(uint16_t*)externPtr + sizeof(uint16_t);
					ptr += sizeof(uint16_t);
				}
			}
		}
		else
		{
			for (uint16_t i = 0; i < keyMeta->m_keyCount; i++)
			{
				const char * value = r->oldColumnOfUpdateType(columnIdxs[i]);
				if (columnInfos[keyMeta->m_types[i]].fixed)
				{
					memcpy(ptr, r->column(columnIdxs[i]), columnInfos[keyMeta->m_types[i]].columnTypeSize);
					ptr += columnInfos[keyMeta->m_types[i]].columnTypeSize;
				}
				else
				{
					const char * v = r->column(columnIdxs[i]);
					*(uint16_t*)ptr = externPtr - key;
					*(uint16_t*)externPtr = r->oldVarColumnSizeOfUpdateType(i, v);
					memcpy(externPtr + sizeof(uint16_t), v, *(uint16_t*)externPtr);
					externPtr += *(uint16_t*)externPtr + sizeof(uint16_t);
					ptr += sizeof(uint16_t);
				}
			}
		}
		return key;
	}
    void appendingIndex::appendUint8Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<uint8_t> c;
        if(r->head->type == DATABASE_INCREASE::R_INSERT|| r->head->type == DATABASE_INCREASE::R_DELETE)
        {
            c.key = *(uint8_t*)r->column(index->m_columnIdxs[0]);
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::R_UPDATE||r->head->type == DATABASE_INCREASE::R_REPLACE)
        {
            c.key = *(uint8_t*)r->column(index->m_columnIdxs[0]);
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key = *(uint8_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }

    void appendingIndex::appendInt8Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<int8_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(int8_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(int8_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(int8_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendUint16Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<uint16_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(uint16_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(uint16_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(uint16_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendInt16Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
		KeyTemplate<int16_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(int16_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(int16_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(int16_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendUint32Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<uint32_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(uint32_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(uint32_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(uint32_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendInt32Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<int32_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(int32_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(int32_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(int32_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendUint64Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<uint64_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(uint64_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(uint64_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(uint64_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendInt64Index(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<int64_t> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(int64_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(int64_t*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(int64_t*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendFloatIndex(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<float> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(float*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(float*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(float*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendDoubleIndex(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<double> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
			c.key = *(double*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
		}
		else if (r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)
		{
			c.key = *(double*)r->column(index->m_columnIdxs[0]);
			appendIndex(index, r, &c, id, false);
			if (r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
			{
				c.key = *(double*)r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				appendIndex(index, r, &c, id, true);
			}
		}
		else
			abort();
    }
    void appendingIndex::appendBinaryIndex(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<binaryType> c;
		if (r->head->type == DATABASE_INCREASE::R_INSERT || r->head->type == DATABASE_INCREASE::R_DELETE)
		{
            c.key.data = r->column(index->m_columnIdxs[0]);
			c.key.size = r->varColumnSize(index->m_columnIdxs[0]);
			index->m_varSize += c.key.size+sizeof(uint16_t);
            appendIndex(index,r,&c,id,false);
        }
        else if(r->head->type == DATABASE_INCREASE::R_UPDATE||r->head->type == DATABASE_INCREASE::R_REPLACE)
        {
            c.key.data = r->column(index->m_columnIdxs[0]);
            appendIndex(index,r,&c,id,false);
            if(r->isKeyUpdated(index->m_columnIdxs,index->m_columnCount))
            {
                c.key.data = r->oldColumnOfUpdateType(index->m_columnIdxs[0]);
				c.key.size = r->oldVarColumnSizeOfUpdateType(index->m_columnIdxs[0],c.key.data);
                appendIndex(index,r,&c,id,true);
            }
        }
        else
            abort();
    }
    void appendingIndex::appendUnionIndex(appendingIndex * index,const DATABASE_INCREASE::DMLRecord * r,uint32_t id)
    {
        KeyTemplate<unionKey> c;
		c.key.key = unionKey::initKey(index->m_arena, &index->m_ukMeta, index->m_columnIdxs, index->m_columnCount, r, false);
		if (!index->m_ukMeta.m_fixed)
			index->m_varSize += *(uint16_t*)(c.key.key + index->m_ukMeta.m_size + sizeof(uint16_t));
		c.key.meta = &index->m_ukMeta;
		appendIndex(index, r, &c, id, false);
		if((r->head->type == DATABASE_INCREASE::R_UPDATE || r->head->type == DATABASE_INCREASE::R_REPLACE)&&r->isKeyUpdated(index->m_columnIdxs, index->m_columnCount))
        {
			c.key.key = unionKey::initKey(index->m_arena, &index->m_ukMeta, index->m_columnIdxs, index->m_columnCount, r, true);
			appendIndex(index, r, &c, id, true);
        }
    }
    appendingIndex::appendingIndex(uint16_t *columnIdxs,uint16_t columnCount,tableMeta * meta,leveldb::Arena *arena ):
		m_columnIdxs(columnIdxs),m_columnCount(columnCount),m_meta(meta),m_arena(arena),m_localArena(arena!=nullptr), m_allCount(0), m_keyCount(0),m_varSize(0)
    {
        if(m_arena == nullptr)
            m_arena = new leveldb::Arena();
		if (columnCount > 0)
		{
			if (!m_ukMeta.init(columnIdxs, columnCount, meta))
				return;
			m_type = UNION;
		}
		else
		{
			m_type = meta->m_columns[columnIdxs[0]].m_columnType;
		}
        switch(m_type)
        {
        case INT8:
			m_comp = new keyComparator<int8_t>;
			m_index = new leveldb::SkipList< KeyTemplate<int8_t>*, keyComparator<int8_t> >(*static_cast<keyComparator<int8_t>*>(m_comp), m_arena);
			break;
		case UINT8:
			m_comp = new keyComparator<uint8_t>;
			m_index = new leveldb::SkipList< KeyTemplate<uint8_t>*, keyComparator<uint8_t> >(*static_cast<keyComparator<uint8_t>*>(m_comp), m_arena);
			break;
        case INT16:
			m_comp = new keyComparator<int16_t>;
			m_index = new leveldb::SkipList< KeyTemplate<int16_t>*, keyComparator<int16_t> >(*static_cast<keyComparator<int16_t>*>(m_comp), m_arena);
			break;
		case UINT16:
			m_comp = new keyComparator<uint16_t>;
			m_index = new leveldb::SkipList< KeyTemplate<uint16_t>*, keyComparator<uint16_t> >(*static_cast<keyComparator<uint16_t>*>(m_comp), m_arena);
			break;
		case INT32:
			m_comp = new keyComparator<int32_t>;
			m_index = new leveldb::SkipList< KeyTemplate<int32_t>*, keyComparator<int32_t> >(*static_cast<keyComparator<int32_t>*>(m_comp), m_arena);
			break;
		case UINT32:
			m_comp = new keyComparator<uint32_t>;
			m_index = new leveldb::SkipList< KeyTemplate<uint32_t>*, keyComparator<uint32_t> >(*static_cast<keyComparator<uint32_t>*>(m_comp), m_arena);
			break;
        case INT64:
			m_comp = new keyComparator<int64_t>;
			m_index = new leveldb::SkipList< KeyTemplate<int64_t>*, keyComparator<int64_t> >(*static_cast<keyComparator<int64_t>*>(m_comp), m_arena);
			break;
		case TIMESTAMP:
		case UINT64:
			m_comp = new keyComparator<uint64_t>;
			m_index = new leveldb::SkipList< KeyTemplate<uint64_t>*, keyComparator<uint64_t> >(*static_cast<keyComparator<uint64_t>*>(m_comp), m_arena);
            break;
		case FLOAT:
			m_comp = new keyComparator<float>;
			m_index = new leveldb::SkipList< KeyTemplate<float>*, keyComparator<float> >(*static_cast<keyComparator<float>*>(m_comp), m_arena);
			break;
		case DOUBLE:
			m_comp = new keyComparator<double>;
			m_index = new leveldb::SkipList< KeyTemplate<double>*, keyComparator<double> >(*static_cast<keyComparator<double>*>(m_comp), m_arena);
			break;
		case BLOB:
		case STRING:
			m_comp = new keyComparator<binaryType>;
			m_index = new leveldb::SkipList< KeyTemplate<binaryType>*, keyComparator<binaryType> >(*static_cast<keyComparator<binaryType>*>(m_comp), m_arena);
			break;
		case UNION:
			m_comp = new keyComparator<unionKey>;
			m_index = new leveldb::SkipList< KeyTemplate<unionKey>*, keyComparator<unionKey> >(*static_cast<keyComparator<unionKey>*>(m_comp), m_arena);
			break;
        default:
			abort();
        }

    };
    appendingIndex::~appendingIndex()
    {
        if(m_index!=nullptr)
        {
            switch(m_type)
            {
            case INT8:
                delete static_cast<leveldb::SkipList< KeyTemplate<int8_t>*,keyComparator<int8_t> >*>(m_index);
                break;
            case UINT8:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint8_t>*,keyComparator<uint8_t> >*>(m_index);
                break;
            case INT16:
                delete static_cast<leveldb::SkipList< KeyTemplate<int16_t>*,keyComparator<int16_t> >*>(m_index);
                break;
            case UINT16:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint16_t>*,keyComparator<uint16_t> >*>(m_index);
                break;
            case INT32:
                delete static_cast<leveldb::SkipList< KeyTemplate<int32_t>*,keyComparator<int32_t> >*>(m_index);
                break;
            case UINT32:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint32_t>*,keyComparator<uint32_t> >*>(m_index);
                break;
            case INT64:
                delete static_cast<leveldb::SkipList< KeyTemplate<int64_t>*,keyComparator<int64_t> >*>(m_index);
                break;
			case TIMESTAMP:
            case UINT64:
                delete static_cast<leveldb::SkipList< KeyTemplate<uint64_t>*,keyComparator<uint64_t> >*>(m_index);
                break;
            case FLOAT:
                delete static_cast<leveldb::SkipList< KeyTemplate<float>*,keyComparator<float> >*>(m_index);
                break;
            case DOUBLE:
                delete static_cast<leveldb::SkipList< KeyTemplate<double>*,keyComparator<double> >*>(m_index);
                break;
            case STRING:
			case BLOB:
                delete static_cast<leveldb::SkipList< KeyTemplate<binaryType>*,keyComparator<binaryType> >*>(m_index);
                break;
			case UNION:
				delete static_cast<leveldb::SkipList< KeyTemplate<unionKey>*, keyComparator<unionKey> >*>(m_index);
				break;
            default:
                abort();
            }
        }
        if(m_localArena&&m_arena!=nullptr)
            delete m_arena;

    }
    typename appendingIndex::appendIndexFunc appendingIndex::m_appendIndexFuncs[] = {
		appendUnionIndex,
		appendUint8Index,appendInt8Index,appendUint16Index,appendInt16Index,
		appendUint32Index,appendInt32Index,appendUint64Index,appendInt64Index,nullptr,
		appendFloatIndex,appendDoubleIndex,nullptr,appendUint64Index,nullptr,nullptr,nullptr,nullptr,
		appendBinaryIndex ,appendBinaryIndex,nullptr,nullptr,nullptr,nullptr };
    void  appendingIndex::append(const DATABASE_INCREASE::DMLRecord  * r,uint32_t id)
    {
        m_appendIndexFuncs[m_type](this,r,id);
		m_allCount++;
    }
}



