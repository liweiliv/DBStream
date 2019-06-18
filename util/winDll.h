#pragma once
#ifndef OS_WIN
#define DLL_EXPORT
#define __declspec(thread)
#endif 
#ifdef OS_WIN
#define DLL_EXPORT __declspec(dllexport)
#endif
