#include "common/config.h"

#include "common/debug.h"
#include "objclass/objclass.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include <iostream>

#define dout_subsys ceph_subsys_objclass

int cls_log(const char *format, ...)
{
   int size = 256, n;
   va_list ap;
   while (1) {
     char buf[size];
     va_start(ap, format);
     n = vsnprintf(buf, size, format, ap);
     va_end(ap);
#define MAX_SIZE 8196
     if ((n > -1 && n < size) || size > MAX_SIZE) {
       dout(1) << buf << dendl;
       return n;
     }
     size *= 2;
   }
}
