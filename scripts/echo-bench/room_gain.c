#include "micgain.h"
#include <stdio.h>
#include <stdlib.h>
int main(int c,char**v){struct micgain g;micgain_init(&g,-26);micgain_start(&g,atof(v[1]));if(atof(v[2])>0)g.talker_min_db=micgain_talker_db(&g)-atof(v[2]);
short b[1024];size_t n;while((n=fread(b,2,1024,stdin))){micgain_run(&g,b,b,n);fwrite(b,2,n,stdout);}fprintf(stderr,"%.1f\n",g.gain_db);}
