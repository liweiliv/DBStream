#pragma once
#ifndef OS_WIN
#define DLL_EXPORT
#define DLL_IMPORT
#endif 
#ifdef OS_WIN
#define DLL_EXPORT __declspec(dllexport)
#define DLL_IMPORT __declspec(dllimport)
#endif
