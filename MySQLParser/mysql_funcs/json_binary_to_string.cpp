/*
 * json_binary_to_string.cpp
 *
 *  Created on: 2018年4月2日
 *      Author: liwei
 */
#include "json_binary.h"
#include "json_binary_to_string.h"
#include "itoa.h"
#include "my_time.h"
#include "binary_log_types.h"
#include "base64.h"
#include <m_string.h>
#include "my_decimal.h"

enum enum_json_type {
  J_NULL,
  J_DECIMAL,
  J_INT,
  J_UINT,
  J_DOUBLE,
  J_STRING,
  J_OBJECT,
  J_ARRAY,
  J_BOOLEAN,
  J_DATE,
  J_TIME,
  J_DATETIME,
  J_TIMESTAMP,
  J_OPAQUE,
  J_ERROR
};
static enum_json_type
bjson2json(const json_binary::Value::enum_type bintype)
{
  enum_json_type res= enum_json_type::J_ERROR;

  switch (bintype)
  {
  case json_binary::Value::V_STRING:
    res= enum_json_type::J_STRING;
    break;
  case json_binary::Value::INT:
    res= enum_json_type::J_INT;
    break;
  case json_binary::Value::UINT:
    res= enum_json_type::J_UINT;
    break;
  case json_binary::Value::DOUBLE:
    res= enum_json_type::J_DOUBLE;
    break;
  case json_binary::Value::LITERAL_TRUE:
  case json_binary::Value::LITERAL_FALSE:
    res= enum_json_type::J_BOOLEAN;
    break;
  case json_binary::Value::LITERAL_NULL:
    res= enum_json_type::J_NULL;
    break;
  case json_binary::Value::ARRAY:
    res= enum_json_type::J_ARRAY;
    break;
  case json_binary::Value::OBJECT:
    res= enum_json_type::J_OBJECT;
    break;
  case json_binary::Value::ERROR:
    res= enum_json_type::J_ERROR;
    break;
  case json_binary::Value::OPAQUE:
    res= enum_json_type::J_OPAQUE;
    break;
  }

  return res;
}

enum_json_type json_type(json_binary::Value & value)
{
	json_binary::Value::enum_type typ= value.type();

	  if (typ == json_binary::Value::OPAQUE)
	  {
	    const enum_field_types ftyp= value.field_type();

	    switch (ftyp)
	    {
	    case MYSQL_TYPE_NEWDECIMAL:
	      return enum_json_type::J_DECIMAL;
	    case MYSQL_TYPE_DATETIME:
	      return enum_json_type::J_DATETIME;
	    case MYSQL_TYPE_DATE:
	      return enum_json_type::J_DATE;
	    case MYSQL_TYPE_TIME:
	      return enum_json_type::J_TIME;
	    case MYSQL_TYPE_TIMESTAMP:
	      return enum_json_type::J_TIMESTAMP;
	    default: ;
	      // ok, fall through
	    }
	  }

	  return bjson2json(typ);
}
static void TIME_from_longlong_packed(MYSQL_TIME *ltime,
                               enum enum_field_types type,
                               longlong packed_value)
{
  switch (type)
  {
  case MYSQL_TYPE_TIME:
    TIME_from_longlong_time_packed(ltime, packed_value);
    break;
  case MYSQL_TYPE_DATE:
    TIME_from_longlong_date_packed(ltime, packed_value);
    break;
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_TIMESTAMP:
    TIME_from_longlong_datetime_packed(ltime, packed_value);
    break;
  default:
    DBUG_ASSERT(0);
    set_zero_time(ltime, MYSQL_TIMESTAMP_ERROR);
    break;
  }
}
bool convert_from_binary_to_decimal(const char *bin, size_t len,
                                       my_decimal *dec)
{
  // Expect at least two bytes, which contain precision and scale.
  bool error= (len < 2);

  if (!error)
  {
    int precision= bin[0];
    int scale= bin[1];

    // The decimal value is encoded after the two precision/scale bytes.
    size_t bin_size= my_decimal_get_binary_size(precision, scale);
    error=
      (bin_size != len - 2) ||
      (binary2my_decimal(E_DEC_ERROR,
    		  (const uchar*)(bin) + 2,
                         dec, precision, scale) != E_DEC_OK);
  }
  return error;
}
/**
  Perform quoting on a JSON string to make an external representation
  of it. it wraps double quotes (text quotes) around the string (cptr)
  an also performs escaping according to the following table:
  <pre>
  Common name     C-style  Original unescaped     Transformed to
                  escape   UTF-8 bytes            escape sequence
                  notation                        in UTF-8 bytes
  ---------------------------------------------------------------
  quote           \"       %x22                    %x5C %x22
  backslash       \\       %x5C                    %x5C %x5C
  backspace       \b       %x08                    %x5C %x62
  formfeed        \f       %x0C                    %x5C %x66
  linefeed        \n       %x0A                    %x5C %x6E
  carriage-return \r       %x0D                    %x5C %x72
  tab             \t       %x09                    %x5C %x74
  unicode         \uXXXX  A hex number in the      %x5C %x75
                          range of 00-1F,          followed by
                          except for the ones      4 hex digits
                          handled above (backspace,
                          formfeed, linefeed,
                          carriage-return,
                          and tab).
  ---------------------------------------------------------------
  </pre>

  @param[in] cptr pointer to string data
  @param[in] length the length of the string
  @param[in,out] buf the destination buffer
  @retval false on success
  @retval true on error
*/
size_t double_quote(const char *cptr, size_t length, char *buf)
{
  buf[0] = '"';
  uint32_t size = 1;
  for (size_t i= 0; i < length; i++)
  {
    bool done= true;
    char esc;
    switch (cptr[i])
    {
    case '"' :
    case '\\' :
      break;
    case '\b':
      esc= 'b';
      break;
    case '\f':
      esc= 'f';
      break;
    case '\n':
      esc= 'n';
      break;
    case '\r':
      esc= 'r';
      break;
    case '\t':
      esc= 't';
      break;
    default:
      done= false;
    }

    if (done)
    {
    	buf[size++] = '\\';
    	buf[size++] = esc;
    }
    else if (((cptr[i] & ~0x7f) == 0) && // bit 8 not set
             (cptr[i] < 0x1f))
    {
      /*
        Unprintable control character, use hex a hexadecimal number.
        The meaning of such a number determined by ISO/IEC 10646.
      */
    	memcpy(buf+size,"\\u00",sizeof("\\u00")-1);
    	size += sizeof("\\u00")-1;
    	buf[size++]=_dig_vec_lower[(cptr[i] & 0xf0) >> 4];
    	buf[size++]=_dig_vec_lower[(cptr[i] & 0x0f)];
    }
    else
    {
    	  buf[size++]=cptr[i];
    }
  }
  buf[size++]='"';
  return size;
}

bool json_to_str(json_binary::Value * value,char * str,uint32_t &size)
{
	 enum_json_type type = json_type(*value);
    enum_field_types ftyp= MYSQL_TYPE_NULL;

	  switch (type)
	  {
	  case enum_json_type::J_TIME:
	  case enum_json_type::J_DATE:
	  case enum_json_type::J_DATETIME:
	  case enum_json_type::J_TIMESTAMP:
	{
		//todo  at least have MAX_DATE_STRING_REP_LENGTH + 2 byte
		MYSQL_TIME t;
		switch (type)
		{
		case enum_json_type::J_DATE:
			ftyp = MYSQL_TYPE_DATE;
			break;
		case enum_json_type::J_DATETIME:
		case enum_json_type::J_TIMESTAMP:
			break;
		case enum_json_type::J_TIME:
			ftyp = MYSQL_TYPE_TIME;
			break;
		default:
			abort();
		}
		TIME_from_longlong_packed(&t, ftyp, (uint64_t) value->get_data());
		str[0] = '"'; /* purecov: inspected */
		size = 1 + my_TIME_to_str(&t, str + 1, 6);
		str[size++] = '"';/* purecov: inspected */
		break;
	}
	  case enum_json_type::J_ARRAY:
	    {
	      /*
	        Reserve some space up front. We know we need at least 3 bytes
	        per array element (at least one byte for the element, one byte
	        for the comma, and one byte for the space).
	      */
	      size_t array_len= value->element_count();
	      //todo at least have 3 * array_len byte
	      str[0]='[';                       /* purecov: inspected */
	      size = 1;
	      for (uint32 i= 0; i < array_len; ++i)
	      {
	    	  //todo at least have 2 byte
	        if(i>0)
	        {
	        	str[size++] = ',';
	        	str[size++] = ' ';
	        }
	        uint32_t _size = 0;
	        json_binary::Value _value(value->element(i));
	        if (!json_to_str(&_value, str+size, _size))
	          return false;
	        size += _size;
	      }
	      str[size++] = ']';
	      break;
	    }
	  case enum_json_type::J_BOOLEAN:
	    {
	    	if(value->type() == json_binary::Value::LITERAL_TRUE)
	    	{
	    		memcpy(str,"true",size=4);	    	  //todo at least have 4 byte
	    	}
	    	else
	    	{
	    		memcpy(str,"false",size=5);	    	  //todo at least have 5 byte
	    	}
	      break;
	    }
	  case enum_json_type::J_DECIMAL:
	    {
	      int length= DECIMAL_MAX_STR_LENGTH + 1;
	      //todo at least have DECIMAL_MAX_STR_LENGTH+1 byte
	      my_decimal m;
	      if (convert_from_binary_to_decimal(value->get_data(),value->get_data_length(),&m) ||
	          decimal2string(&m, str, &length, 0, 0, 0))
	        return false;                           /* purecov: inspected */
	      size = length;
	      break;
	    }
	  case enum_json_type::J_DOUBLE:
	    {
		   //todo at least have MY_GCVT_MAX_FIELD_WIDTH+1 byte
	      double d= value->get_double();
	      size = my_gcvt(d, MY_GCVT_ARG_DOUBLE, MY_GCVT_MAX_FIELD_WIDTH,str,NULL);
	      break;
	    }
	  case enum_json_type::J_INT:
	    {
			//todo at least have MAX_BIGINT_WIDTH+1 byte
	    	size = ltoa(value->get_int64(),str);
	      break;
	    }
	  case enum_json_type::J_NULL:
		  //todo at least have 4 byte
	    memcpy(str,"null",size=4);
	    break;
	  case enum_json_type::J_OBJECT:
	    {
	      /*
	        Reserve some space up front to reduce the number of
	        reallocations needed. We know we need at least seven bytes per
	        member in the object. Two bytes for the quotes around the key
	        name, two bytes for the colon and space between the key and
	        the value, one byte for the value, and two bytes for the comma
	        and space between members. We're generous and assume at least
	        one byte in the key name as well, so we reserve eight bytes per
	        member.
	      */
			//todo at least have 2 + 8 * value.element_count()  byte
	    	str[size++] = '{';
	      uint32 i= 0;
	      for (int idx = 0;idx<value->element_count();idx++)
	      {
	    	  if(idx>0)//todo at least have 2 byte
	    	  {
	    		  str[size++] = ',';
	    		  str[size++] = ' ';
	    	  }
	        //todo at least have key_length + 4 byte
	        size += double_quote(value->key(idx).get_data(),value->key(idx).get_data_length(),str+size);
		str[size++] = ':';
		str[size++] = ' ';
	        uint32_t _size = 0;
	        json_binary::Value _value = value->element(idx);
	        if(!json_to_str(&_value,str+size,_size))
	        	return false;
	        size += _size;
	      }
	      str[size++] = '}';
	      break;
	    }
	  case enum_json_type::J_OPAQUE:
	    {
	      if (value->get_data_length() > base64_encode_max_arg_length())
	    	  return false;

	      const size_t needed=
	        static_cast<size_t>(base64_needed_encoded_length(value->get_data_length()));

	      str[0]='"';
	      memcpy(str+1,"base64:type",sizeof("base64:type")-1);
	      size = sizeof("base64:type");//equal 1 + (sizeof("base64:type")-1)
	      size+=ultoa(value->field_type(),str+size);
	      const char *prefix= "base64:type";
	      str[size++] = ':';
	      // "base64:typeXX:<binary data>"
	      base64_encode(value->get_data(), value->get_data_length(),const_cast<char*>(str+size));
	      size+=needed - 1;
	      str[size++] = '"';
	      break;
	    }
	  case enum_json_type::J_STRING:
	    {
	      size = double_quote(value->get_data(),value->get_data_length(),str);
	      break;
	    }
	  case enum_json_type::J_UINT:
	    {
			//todo at least have MAX_BIGINT_WIDTH+1 byte
	    	size = utoa(value->get_uint64(),str);
	      break;
	    }
	  default:
	    return false;
	  }
	  return true;
}



