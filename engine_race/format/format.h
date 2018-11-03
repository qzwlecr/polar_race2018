#ifndef ENGINE_RACE_FORMAT_FORMAT_H
#define ENGINE_RACE_FORMAT_FORMAT_H

#include <cstdio>
#include <mutex>
#include <string>
#include <sstream>

#define Q_ESCAPE_CHAR "\x1b"
#define Q_FMT_APPLY(x) Q_ESCAPE_CHAR x

// colors
#define Q_COLOR_RESET "[0m"
#define Q_COLOR_RED "[31m"
#define Q_COLOR_GREEN "[32m"
#define Q_COLOR_YELLOW "[33m"
#define Q_COLOR_BLUE "[34m"
#define Q_COLOR_MAGENTA "[35m" // purple-like
#define Q_COLOR_CYAN "[36m"
#define Q_COLOR_WHITE "[37m"

extern std::mutex logMu;

// functions for better usage?
void qfmtColor(const char *color_const);

void qfmtClearColor();
// qfmtColorizer("[",FAIL,"]",Q_COLOR_RED) ==> try this out!
#define qfmtColorizer(prefix, colored, suffix, color) qfmtColorizerF(stdout,prefix,colored,suffix,color)

void qfmtColorizerF(FILE *dstfile, const char *prefix, const char *colored, const char *suffix, const char *color);

// const moves

std::string KVArrayDump(const char* arr, size_t dumplen);


#endif
