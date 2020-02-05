#pragma once
namespace STORE
{
	static constexpr auto STORE_SECTION = "store";
	static constexpr auto STORE_MEM_ALLOCER = "memAllocer";
	static constexpr auto STORE_BUDDY_MEM_ALLOCER = "buddy";
	static constexpr auto STORE_SYSTEM_MEM_ALLOCER = "system";

	static constexpr auto STORE_MAX_MEM = "maxMem";

	static constexpr auto STORE_MAX_MEM_DEFAULT = 512 * 1024 * 1024;
	static constexpr auto STORE_MAX_MEM_MIN = 128 * 1024 * 1024;
	static constexpr auto STORE_MAX_MEM_MAX = 256LL * 1024 * 1024 * 1024;

#define MAIN_STREAM "mainStream"
#define GENERATED_STREAM "generatedStream"

}