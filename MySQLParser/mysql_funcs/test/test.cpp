#include "json_binary.h"
#include "json_binary_to_string.h"
int main()
{
    char t[256]="\x00\x06\x00S\x00.\x00\x04\x002\x00\x04\x006\x00\x04\x00:\x00\x07\x00A\x00\x09\x00J\x00\x09\x00\x05\x00\x00\x05\x00\x00\x05\x00\x00\x05\x00\x00\x05\x00\x00\x05\x01\x00key1key2key3bssFlagbatchFlagonwayFlag";
    char t1[256]={0};
    uint64_t size =0;
    json_binary::Value v= json_binary::parse_binary(t, strlen(t));
    json_to_str(&v,t1,size);
    printf("%s\n",t1);
}
