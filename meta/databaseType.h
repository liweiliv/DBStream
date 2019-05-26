/*
 * databaseType.h
 *
 *  Created on: 2019年5月18日
 *      Author: liwei
 */

#ifndef META_DATABASETYPE_H_
#define META_DATABASETYPE_H_
#include <string>
#include "metaData.h"
namespace META{
	class databaseType{
		std::string m_databaseType;
		databaseType(std::string databaseType):m_databaseType(databaseType){}
		virtual ~databaseType(){}
		virtual std::string columnString(const columnMeta &column) const  = 0;
	};
}





#endif /* META_DATABASETYPE_H_ */
