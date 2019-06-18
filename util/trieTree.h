/*
 * trieTree.h
 *
 *  Created on: 2018年11月6日
 *      Author: liwei
 */

#ifndef _trieTree_H_
#define _trieTree_H_
#include <stdint.h>
#include <stdlib.h>
#include "winDll.h"
class  trieTree
{
private:
    struct node
    {
        uint8_t c;
        uint8_t num;
        void ** child;
        inline void * get(uint8_t c);
        inline bool put(uint8_t c,void *value);
        node(uint8_t _c);
        ~node();
        class iterator
        {
        private:
            friend struct node;
            node * m_node;
            uint8_t h;
            void ** m_mid;
            uint8_t m;
            void ** m_low;
            uint8_t l;
        public:
            iterator();
            iterator(const iterator &iter);
            iterator &operator =(const iterator &iter);
            bool valid();
            void clear();
            void * value();
            const node * getNode();
            uint8_t key();
            bool next();
        };
        iterator begin();
    };
    node  m_root;
    uint32_t m_nodeCount;
    uint32_t m_valueCount;
    int (*m_valueDestroyFunc)(void* value);
public:
    class iterator
    {
    public:
        struct stacks
        {
            node::iterator nodeIter;
            stacks * parent;
        };
    private:
        friend class trieTree;
        stacks m_stack;
        stacks * m_top;
        uint16_t keyStackTop;
        uint8_t keyStack[256];
    public:
		DLL_EXPORT iterator();
		DLL_EXPORT iterator(const iterator & iter);
		DLL_EXPORT iterator &operator =(const iterator &iter);
		DLL_EXPORT~iterator();
		DLL_EXPORT void clear();
		DLL_EXPORT bool valid();
		DLL_EXPORT void * value();
		DLL_EXPORT const unsigned char *key();
		DLL_EXPORT bool next();
    };
public:
	DLL_EXPORT iterator begin();
	DLL_EXPORT trieTree(int (*valueDestroyFunc)(void* value) = NULL);
	DLL_EXPORT ~trieTree();
	DLL_EXPORT void clear();
	DLL_EXPORT int insert(const unsigned char * str,void *value);
	DLL_EXPORT int insertNCase(const unsigned char * str,void *value);
	DLL_EXPORT void * find(const unsigned char * str,uint32_t size = 0xffffffffu);
	DLL_EXPORT void * findNCase(const unsigned char * str,uint32_t size = 0xffffffffu);
};


#endif /* _trieTree_H_ */
