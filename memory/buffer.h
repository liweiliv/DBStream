#pragma once
#include <stdint.h>
struct bufferBase {
	char* buffer;
};

class bufferBaseAllocer {
public:
	virtual bufferBase* alloc(uint32_t size) = 0;
	virtual void free(bufferBase* buf) = 0;
};
class defaultBufferBaseAllocer:public bufferBaseAllocer{
public:
	bufferBase* alloc(uint32_t size)
	{
		bufferBase* buffer = (bufferBase*)malloc(sizeof(bufferBase) + size);
		buffer->buffer = ((char*)buffer) + sizeof(bufferBase);
		return buffer;
	}
	void free(bufferBase* buf)
	{
		::free(buf);
	}
};