#pragma once
namespace DB_INSTANCE
{
	static constexpr auto INSTANCE_SECTION = "instance";
	static constexpr auto INSTANCE_MEM_ALLOCER = "memAllocer";
	static constexpr auto INSTANCE_BUDDY_MEM_ALLOCER = "buddy";
	static constexpr auto INSTANCE_SYSTEM_MEM_ALLOCER = "system";

	static constexpr auto INSTANCE_MAX_MEM = "maxMem";

	static constexpr auto INSTANCE_MAX_MEM_DEFAULT = 512 * 1024 * 1024;
	static constexpr auto INSTANCE_MAX_MEM_MIN = 128 * 1024 * 1024;
	static constexpr auto INSTANCE_MAX_MEM_MAX = 256LL * 1024 * 1024 * 1024;

#define MAIN_STREAM "mainStream"
#define GENERATED_STREAM "generatedStream"

}