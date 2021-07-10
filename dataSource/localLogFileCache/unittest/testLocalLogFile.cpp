#include "../localLogFileCache.h"
namespace DATA_SOURCE
{
	int test()
	{
		Config conf;
		LocalLogFileCache cache(&conf);
		if (!dsCheck(cache.init()))
		{
			LOG(ERROR) << getLocalStatus().toString();
			abort();
		}
		cache.start();
		logFile* f;
		cache.createNextLogFile(1, f);
		std::thread w([&cache,&f]()->void {
			for (int i = 0; i < 10000000; i++)
			{
				LogEntry * e = cache.allocNextRecord(128);
				memset(e, 0, 128);
				e->size = 128;
				e->getCheckpoint()->srcPosition = i + 1;
				cache.recordSetted();
			}
			});
		
		std::thread r([&cache]()->void {
			LogEntry* e;
			LocalLogFileCache::iterator iter(&cache);
			uint64_t id = 0;

			if (!dsCheck(iter.seekToBegin(e)))
			{
				LOG(ERROR) << getLocalStatus().toString();
				abort();
			}
			if (e != nullptr && e->getCheckpoint()->srcPosition != id + 1)
			{
				LOG(ERROR) << "log id do not start from 1";
				abort();
			}
			do {
				if (e != nullptr)
				{
					if (e->getCheckpoint()->srcPosition != id + 1)
					{
						LOG(ERROR) << "log id is not" << id + 1;
						abort();
					}
					if (++id == 10000000)
						break;
					cache.setPurgeTo(e->getCheckpoint()->seqNo.seqNo);
				}
			} while (dsCheck(iter.next(e, 1000)));
		});
		
		w.join();
		r.join();
		cache.stop();
		return 0;
	}

}
int main()
{
	DATA_SOURCE::test();
	return 0;
}