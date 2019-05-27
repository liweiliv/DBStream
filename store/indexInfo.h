#pragma once
#include <stdint.h>
#include "../meta/columnType.h"
namespace leveldb {
	class Arena;
}
namespace DATABASE_INCREASE {
	struct DMLRecord;
}
namespace META {
	struct tableMeta;
}
namespace STORE {
	struct binaryType {
		uint16_t size;
		const char * data;
		binaryType();
		binaryType(const char* _data, uint16_t _size);
		binaryType(const binaryType & dest);
		binaryType operator=(const binaryType & dest);
		bool operator< (const binaryType & dest) const;
		int compare(const binaryType & dest) const;
		bool operator> (const binaryType & dest) const;
	};
	struct unionKeyMeta {
		uint16_t m_keyCount;
		uint8_t *m_types;
		bool m_fixed;
		uint16_t m_size;
		unionKeyMeta() :m_keyCount(0), m_types(nullptr) {}
		unionKeyMeta(const uint16_t *columnIndexs, uint16_t columnCount, META::tableMeta* meta) :m_keyCount(0), m_types(nullptr), m_fixed(true), m_size(0) {
			init(columnIndexs, columnCount, meta);
		}
		bool init(const uint16_t *columnIndexs, uint16_t columnCount, META::tableMeta* meta);
		~unionKeyMeta() {
			if (m_types != nullptr)
				delete m_types;
		}
	};
	struct unionKey {
		const char * key;
		const unionKeyMeta * meta;
		unionKey();
		unionKey(const unionKey & dest);
		int compare(const unionKey & dest) const;
		inline bool operator> (const unionKey & dest) const;
		inline static const char* initKey(leveldb::Arena * arena, unionKeyMeta * keyMeta, uint16_t *columnIdxs, uint16_t columnCount, const DATABASE_INCREASE::DMLRecord * r, bool keyUpdated = false);
	};
}
