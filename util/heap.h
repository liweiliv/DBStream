/*
 * heap.h
 *
 *  Created on: 2019年1月17日
 *      Author: liwei
 */

#ifndef HEAP_H_
#define HEAP_H_
#include <stdint.h>
template<typename T, typename Compare>
class heap {
protected:
	Compare compare;
	uint32_t maxSize;
	uint32_t currentSize;
	T* h;
	inline void adjust(int start, int end)
	{
		int p = start, c = left(start);
		while (c <= end)
		{
			if (c + 1 < end && compare(h[c + 1], h[c]))
				c++;
			if (compare(h[p], h[c]))
				return;
			swap(p, c);
			p = c;
			c = left(p);
		}
	}
	static inline int parent(int i)
	{
		return i >> 1;
	}
	static inline int left(int i)
	{
		return i << 1;
	}
	static inline int right(int i)
	{
		return 1 + (i << 1);
	}
	inline void swap(int src, int dest)
	{
		T tmp = h[dest];
		h[dest] = h[src];
		h[src] = tmp;
	}
public:
	heap(uint32_t heapSize) :h(nullptr), maxSize(heapSize), currentSize(0) {
		h = new T[heapSize + 1];
	}
	void set(const T* data, int size)
	{
		for (int i = 0; i < sizei++)
			h[i+1] = data[i];
		currentSize = size + 1;
	}
	void clear()
	{
		currentSize = 0;
	}
	virtual ~heap()
	{
		delete[]h;
	}
	inline void sort()
	{
		int i;
		for (i = parent(currentSize) - 1; i >= 1; i--)
			adjust(i, currentSize - 1);
		for (i = currentSize - 1; i > 1; i--)
		{
			swap(0, i);
			adjust(0, i - 1);
		}
	}
	inline uint32_t size()
	{
		return currentSize;
	}
	inline const T& get()
	{
		return h[1];
	}
	inline int getSecond(T& second)
	{
		if (unlikely(currentSize <= 1))
			return -1;
		if (currentSize == 2 || compare(h[2], h[3]))
		{
			second = h[2];
			return 1;
		}
		else
		{
			second = h[3];
			return 2;
		}
	}
	inline void replaceAt(const T& data, int pos)
	{
		h[pos+1] = data;
		adjust(pos+1, currentSize);
		if (currentSize == 0)
			currentSize = 1;
	}
	inline void popAt(int pos)
	{
		if (currentSize <= 1)
		{
			currentSize = 0;
			return;
		}
		h[pos+1] = h[currentSize];
		adjust(pos+1, currentSize);
		currentSize--;
	}
	inline void insert(const T& data)
	{
		h[++currentSize] = data;
		for (int i = currentSize, p = parent(i); i > 1; i = p, p = parent(i))
		{
			if (compare(h[i], h[p]))
				swap(p, i);
			else
				return;
		}
#ifdef DEBUG
		print();
#endif
	}
	void print()
	{
		for (int i = 1; i <= currentSize; i++)
			printf("%d ", h[i]);
		printf("\n");
	}
	inline void pop()
	{
		popAt(0);
#ifdef DEBUG
		print();
#endif
	}
	inline void popAndInsert(const T& data)
	{
		replaceAt(data, 0);
#ifdef DEBUG
		print();
#endif
	}
};
template<class T>
class defaultLessCompare {
public: inline bool operator()(const T& src, const T& dest) { return src < dest; }
};
template<class T>
class defaultGreaterCompare {
public: inline bool operator()(const T& src, const T& dest) { return src > dest; }
};

typedef heap<int16_t, defaultLessCompare<int16_t> > shortMinHeap;
typedef heap<uint16_t, defaultLessCompare<uint16_t> > ushortMinHeap;
typedef heap<int32_t, defaultLessCompare<int> > intMinHeap;
typedef heap<uint32_t, defaultLessCompare<uint32_t> > uintMinHeap;
typedef heap<int64_t, defaultLessCompare<int64_t> > longMinHeap;
typedef heap<uint64_t, defaultLessCompare<uint64_t> > ulongMinHeap;
typedef heap<float, defaultLessCompare<float> > floatMinHeap;
typedef heap<double, defaultLessCompare<double> > doubleMinHeap;

typedef heap<int16_t, defaultGreaterCompare<int16_t> > shortMaxHeap;
typedef heap<uint16_t, defaultGreaterCompare<uint16_t> > ushortMaxHeap;
typedef heap<int32_t, defaultGreaterCompare<int> > intMaxHeap;
typedef heap<uint32_t, defaultGreaterCompare<uint32_t> > uintMaxHeap;
typedef heap<int64_t, defaultGreaterCompare<int64_t> > longMaxHeap;
typedef heap<uint64_t, defaultGreaterCompare<uint64_t> > ulongMaxHeap;
typedef heap<float, defaultGreaterCompare<float> > floatMaxHeap;
typedef heap<double, defaultGreaterCompare<double> > doubleMaxHeap;

#endif /* HEAP_H_ */
