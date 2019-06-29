#pragma once
typedef enum { MY_GCVT_ARG_FLOAT, MY_GCVT_ARG_DOUBLE } my_gcvt_arg_type;
size_t my_fcvt(double x, int precision, char* to, bool* error);
size_t my_fcvt_compact(double x, char* to, bool* error);
size_t my_gcvt(double x, my_gcvt_arg_type type, int width, char* to,
	bool* error);
double my_strtod(const char* str, const char** end, int* error);
double my_atof(const char* nptr);
