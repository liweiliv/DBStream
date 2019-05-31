#pragma once
#ifndef OS_WIN
#define DLL_EXPORT
#endif 
#ifdef OS_WIN
#define DLL_EXPORT __declspec(dllexport)
#endif
