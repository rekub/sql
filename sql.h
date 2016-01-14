#include <mysql/mysqld_error.h>
#include <stdbool.h>
void OpenSQL(const char * mysql_host, const char * mysql_user,
const char * mysql_passwd, const char * mysql_db, unsigned int mysql_port,
const char * mysql_unix_socket);
bool rSQL(const char * q, ...);
unsigned long wSQL(const char * q, ...);
unsigned long lastidSQL(void);
bool aSQL(void);
void * rfSQL(const char * q, ...);
bool rnSQL(void * res, ...);
void sqlerror(void);
void CloseSQL(void);
