#include "format.h"

std::mutex logMu;

void qfmtColor(const char *c) {
    printf("\x1b%s", c);
}

void qfmtClearColor() {
    qfmtColor(Q_COLOR_RESET);
}

void qfmtColorizerF(FILE *f, const char *prefix, const char *colored, const char *suffix, const char *c) {
    fprintf(f, "%s\x1b%s%s\x1b%s%s", prefix, c, colored, Q_COLOR_RESET, suffix);
}

std::string KVArrayDump(const char* arr, size_t len){
    std::string s = "[";
    for(size_t i = 0; i < len; i+=sizeof(uint16_t)){
        char tmp[10] = {0};
        sprintf(tmp, "%hu", *reinterpret_cast<const uint16_t*>(arr + i));
        if(i + 2 < len){
            s = s + tmp + " ";
        } else {
            s = s + tmp + "]";
        }
    }
    return s;
}


