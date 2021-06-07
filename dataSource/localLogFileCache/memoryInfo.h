#pragma once
#include <atomic>
#include <mutex>
#include <chrono>
#include <condition_variable>
namespace DATA_SOURCE
{
	class memoryInfo
	{
	private:
		uint32_t m_defaultShardSize;

		uint64_t m_maxMem;
		uint32_t m_largeDataSize;
		uint64_t m_highFlow;
		uint64_t m_veryHighFlow;

		std::atomic_char m_largeDataCount;
		std::atomic_uint64_t m_largeDataMemused;
		std::atomic_uint64_t m_memused;

		std::mutex m_allocLock;
		std::condition_variable m_allocCond;

		std::mutex m_flushLock;
		std::condition_variable m_flushCond;

		volatile bool m_running;
	public:
		memoryInfo(uint32_t defaultShardSize, uint64_t maxMem) :m_defaultShardSize(defaultShardSize), m_maxMem(maxMem), m_largeDataSize(maxMem / 4), m_running(false)
		{
		}
		bool isRunning()
		{
			return m_running;
		}
		void start()
		{
			m_running = true;
		}
		void stop()
		{
			m_running = false;
		}

		inline uint32_t getDefaultShardSize()
		{
			return m_defaultShardSize;
		}
		inline void waitFlushCond()
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(10);
			std::unique_lock<std::mutex> lock(m_flushLock);
			m_flushCond.wait_until(lock, t);
		}

		inline void signFlushCond()
		{
			m_flushCond.notify_one();
		}

		inline void waitAllocCond()
		{
			auto t = std::chrono::system_clock::now();
			t += std::chrono::milliseconds(10);
			std::unique_lock<std::mutex> lock(m_allocLock);
			m_allocCond.wait_until(lock, t);
		}

		inline void signAllocCond()
		{
			m_allocCond.notify_one();
		}

		inline void addMemSize(uint32_t size)
		{
			if (m_memused.fetch_add(size, std::memory_order_relaxed) > m_highFlow)
				signFlushCond();
		}

		inline void subMemSize(uint32_t size)
		{
			m_memused.fetch_sub(size, std::memory_order_relaxed);
			if (size >= m_largeDataSize)
			{
				m_largeDataMemused.fetch_sub(size, std::memory_order_relaxed);
				m_largeDataCount.fetch_sub(1, std::memory_order_relaxed);
			}
			signAllocCond();
		}

		inline char* allocLargeData(uint32_t size)
		{
			do
			{
				char largeDataCount = m_largeDataCount.load(std::memory_order_relaxed);
				if (largeDataCount > 5)
				{
					signFlushCond();
					waitAllocCond();
					continue;
				}
				if (m_largeDataCount.compare_exchange_weak(largeDataCount, largeDataCount + 1, std::memory_order_relaxed, std::memory_order_relaxed))
				{
					m_largeDataMemused.fetch_add(size, std::memory_order_relaxed);
					addMemSize(size);
					return (char*)malloc(size);
				}
			} while (m_running);
		}

		inline char* alloc(uint32_t size)
		{
			if (size >= m_largeDataSize)
				return allocLargeData(size);
			do
			{
				uint64_t memused = m_memused.load(std::memory_order_relaxed);
				if (memused + size > m_maxMem)
				{
					signFlushCond();
					if (memused - m_largeDataMemused.load(std::memory_order_relaxed) > m_maxMem)
					{
						waitAllocCond();
						continue;
					}
				}
				if (m_memused.compare_exchange_weak(memused, memused + size, std::memory_order_relaxed, std::memory_order_relaxed))
					return (char*)malloc(size);
			} while (m_running);
			return nullptr;
		}

		inline void freeMem(char* buf, uint32_t size)
		{
			free(buf);
			subMemSize(size);
		}

		inline char* realloc(char* buf, uint32_t size, uint32_t newSize)
		{
			freeMem(buf, size);
			return alloc(newSize);
		}

		inline bool flowIsHigh()
		{
			return ((double)m_memused.load(std::memory_order_relaxed)) / m_maxMem >= 0.5f;
		}

		inline bool flowIsVeryHigh()
		{
			return ((double)m_memused.load(std::memory_order_relaxed)) / m_maxMem >= 0.9f || m_maxMem - m_memused.load(std::memory_order_relaxed) < m_defaultShardSize;
		}
	};
}