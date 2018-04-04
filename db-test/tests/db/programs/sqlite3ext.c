/*
 * Compile with:
 *   gcc -g -fPIC -shared sqlite3ext.c -o sqlite3ext.so
 */
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

static void extplus(sqlite3_context *context, int argc, sqlite3_value **argv){
  double a = 0, b = 0;
  int nullresult = 0;

  switch (sqlite3_value_type(argv[0])) {
  case SQLITE_NULL: {
    nullresult = 1;
    break;
  }
  case SQLITE_INTEGER:
  case SQLITE_FLOAT: {
    a = sqlite3_value_double(argv[0]);
    break;
  }
  default: {
    sqlite3_result_error(context, "wanted a number", -1);
    return;
  }
  }

  switch (sqlite3_value_type(argv[1])) {
  case SQLITE_NULL: {
    nullresult = 1;
    break;
  }
  case SQLITE_INTEGER:
  case SQLITE_FLOAT: {
    b = sqlite3_value_double(argv[1]);
    break;
  }
  default: {
    sqlite3_result_error(context, "wanted a number", -1);
    return;
  }
  }

  if (nullresult) {
    sqlite3_result_null(context);
  } else {
    sqlite3_result_double(context, a+b);
  }
}

int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg,
                           const sqlite3_api_routines *pApi){
  SQLITE_EXTENSION_INIT2(pApi);
  sqlite3_create_function(db, "extplus", 2, SQLITE_UTF8, 0, &extplus, 0, 0);
  return 0;
}
