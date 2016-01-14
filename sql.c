#include "error.h"
#include "memory.h"
#include <stdbool.h>
#include <stdlib.h>
#include <mysql/mysql.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

MYSQL * mysql = NULL;
void CloseSQL(void) {
  if (mysql) mysql_close(mysql);
  mysql = NULL;
}

void OpenSQL(const char * mysql_host, const char * mysql_user,
const char * mysql_passwd, const char * mysql_db, unsigned int mysql_port,
const char * mysql_unix_socket) {
  if (mysql = mysql_init(NULL)) {
    mysql_options(mysql,MYSQL_SET_CHARSET_DIR,"/usr/local/share/mysql/charsets");
    mysql_options(mysql,MYSQL_SET_CHARSET_NAME,"koi8r");
    if (mysql_real_connect(mysql,mysql_host,mysql_user,mysql_passwd,mysql_db,
    mysql_port,mysql_unix_socket,CLIENT_FOUND_ROWS)) {
      if (!atexit(CloseSQL)) return;
    }
    else
      error("Failed to connect to database. Error: %s", mysql_error(mysql));
  }
  else errno = ENOMEM;
  error("Cannot open SQL");
}


void Vasprintf(char **ret, const char *format, va_list *ap) {
  va_list apc;
  va_copy(apc,*ap);
  if (-1 == vasprintf(ret,format,apc)) error("Cannot format string: %s",format);
  va_end(apc);
  const char * f = format;
  while (f = strchr(f,'%')) {
    ++f;
    switch (*f) {
      default:
        errno = EDOOFUS;
        error("Unsupported format %c in %s",*f,format);
      case 's':
        (void) va_arg(*ap,char *);
      case '%': break;
      case 'd':
        (void) va_arg(*ap,int);
        break;
      case 'u':
        (void) va_arg(*ap,unsigned);
        break;
      case 'f':
        (void) va_arg(*ap,double);
        break;
    }
    ++f;
  }
}

void sqlerror(void) {
  error("MySQL error: %s.",mysql_error(mysql));
}

void query(const char * q, va_list * ap) {
  unsigned * error_code = 0;
  if (!strncmp("%e",q,2)) {
    error_code = va_arg(*ap,unsigned *);
    q+=2;
    *error_code = 0;
  }
  char * left = NULL;
  while(1) {
    char * qpos = strstr(q,"%q");
    while (qpos) {
      char * qprev = qpos;
      size_t lp = 0;
      while (qprev != q) {
        qprev--;
        if (*qprev == '%') lp++;
        else break;
      }
      if (lp%2) qpos = strstr(qpos+2,"%q");
      else break;
    }
    if (qpos) {
      if (qpos != q) {
        char * middle;
        size_t FL = qpos - q + 1;
        char * format = Malloc(FL);
        memcpy(format,q,FL-1);
        format[FL-1] = 0;
        Vasprintf(&middle,format,ap);
        if (!middle) error(0);
        Free(format);
        Astrcat(&left,middle);
      }
      q = qpos+2;
      const char * qstr = va_arg(*ap,const char *);
      if (qstr) {
        size_t L = strlen(qstr);
        char * eqstr = Malloc(1+2*L);
        (void)mysql_real_escape_string(mysql,eqstr,qstr,L);
        char * qeqstr;
        (void)asprintf(&qeqstr,"'%s'",eqstr);
        if (!qeqstr) error(0);
        Free(eqstr);
        Astrcat(&left,qeqstr);
      }
      else {
        char * null_str = Strdup("NULL");
        Astrcat(&left,null_str);
      }
      if (*q) continue;
    }
    else {
      char * right;
      Vasprintf(&right,q,ap);
      Astrcat(&left,right);
    }
    break;
  }
  if (!left) {
    errno = EDOOFUS;
    error(0);
  }
  if (mysql_real_query(mysql,left,strlen(left))) {
    if (error_code) *error_code = mysql_errno(mysql);
    else error("MySQL error: %s.",mysql_error(mysql));
  }
  Free(left);
}

bool rSQL(const char * q, ...) {
  va_list ap;
  va_start(ap,q);
  query(q,&ap);
  MYSQL_RES * res = mysql_store_result(mysql);
  if (!res) error("MySQL no result, error: %s",mysql_error(mysql));
  bool res_flag = false;
  unsigned num_fields = mysql_num_fields(res);
  MYSQL_FIELD *  fields = mysql_fetch_fields(res);
  MYSQL_ROW row;
  if (row = mysql_fetch_row(res)) {
    res_flag = true;
    for (unsigned i = 0; i< num_fields; ++i) {
      switch (fields[i].type) {
        default:
          errno = EDOOFUS;
          error("Unsupported field type");
        case FIELD_TYPE_TINY:
        case FIELD_TYPE_LONG:
          {
            int32_t* n = va_arg(ap,int32_t*);
            *n = atoi(row[i]);
            break;
          }
        case FIELD_TYPE_LONGLONG:
          {
            long long* n = va_arg(ap,long long*);
            *n = strtoll(row[i],0,10);
            break;
          }
        case FIELD_TYPE_BLOB:
        case FIELD_TYPE_STRING:
        case FIELD_TYPE_VAR_STRING:
          {
            char ** s = va_arg(ap,char**);
            if (row[i]) {
              *s = Malloc(strlen(row[i])+1);
              strcpy(*s,row[i]);
            }
            else *s = 0;
          }
      }
    }
  }
  mysql_free_result(res);
  va_end(ap);
  return res_flag;
}

void wSQL(const char * q, ...) {
  va_list ap;
  va_start(ap,q);
  query(q,&ap);
  va_end(ap);
}

bool aSQL(void) {
  switch (mysql_affected_rows(mysql)) {
    case -1: errno = EDOOFUS; error("Wrong affected query");
    case 0:  return false;
    default: return true;
  }
}

unsigned long lastidSQL(void) {
  unsigned long res = mysql_insert_id(mysql);
  if (!res) {
    errno = EINVAL;
    error("nothing inserted");
  }
  return res;
}

void * rfSQL(const char * q, ...) {
  va_list ap;
  va_start(ap,q);
  query(q,&ap);
  va_end(ap);
  MYSQL_RES * res = mysql_store_result(mysql);
  if (!res) error("MySQL no result, error: %s",mysql_error(mysql));
  return res;
}

bool rnSQL(void * res, ...) {
  if (res) {
    bool res_flag = false;
    va_list ap;
    va_start(ap,res);
    unsigned num_fields = mysql_num_fields(res);
    MYSQL_FIELD *  fields = mysql_fetch_fields(res);
    MYSQL_ROW row;
    if (row = mysql_fetch_row(res)) {
      res_flag = true;
      for (unsigned i = 0; i< num_fields; ++i) {
        switch (fields[i].type) {
          default:
            errno = EDOOFUS;
            error("Unsupported field type");
          case FIELD_TYPE_TINY:
            {
              int8_t* n = va_arg(ap,int8_t*);
              *n = row[i] ? atoi(row[i]) : 0;
              break;
            }
          case FIELD_TYPE_INT24:
          case FIELD_TYPE_LONG:
            {
              int32_t* n = va_arg(ap,int32_t*);
              *n = row[i] ? atoi(row[i]) : 0;
              break;
            }
          case FIELD_TYPE_LONGLONG:
            {
              long long* n = va_arg(ap,long long*);
              *n = strtoll(row[i],0,10);
              break;
            }
          case FIELD_TYPE_BLOB:
          case FIELD_TYPE_STRING:
          case FIELD_TYPE_VAR_STRING:
            {
              char ** s = va_arg(ap,char**);
              if (row[i]) {
                *s = Malloc(strlen(row[i])+1);
                strcpy(*s,row[i]);
              }
              else *s = 0;
            }
        }
      }
    }
    if (!res_flag) mysql_free_result(res);
    va_end(ap);
    return res_flag;
  }
  return false;
}
