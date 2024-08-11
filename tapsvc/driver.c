#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <svc.h>

int main(int argc, char **argv)
{
   char cmd   [1024];
   char status[1024];

   int  nrec;

   strcpy(cmd, "tap \"select rax, dec from PS where ra<3.0\" test.tbl");

   svc_run(cmd);

   strcpy(status, svc_value("stat"));

   if(strcmp(status, "OK") == 0)
   {
      nrec = atoi(svc_value("nrec"));
      printf("nrec = %d\n", nrec);
      fflush(stdout);
   }
   else
   {
      printf("msg: %s\n", svc_value("msg"));
      fflush(stdout);
   }

   exit(0);
}
