#define _POSIX_C_SOURCE 200809L
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#define KEY ((key_t)0x52545232)
#define MAGIC 0x4D515254u
#define MAX 256
#define STD 1
#define PRI 2
#define MT 1L
#define TIMEOUT 10

typedef struct { long type; unsigned magic, version, cls; unsigned long long id; int reply; unsigned len; char data[MAX]; } Request;
typedef struct { long type; unsigned magic; unsigned long long id; int reply; unsigned cls; int status; char text[MAX]; } Response;
typedef struct { long type; unsigned magic; unsigned long long id; int reply; unsigned len; char data[MAX]; } Work;
static volatile sig_atomic_t stop;
static int rq=-1,wq[2]={-1,-1},sq=-1,alive[2];
static pid_t wp[2]={-1,-1};
static unsigned long recv_n[2],sent_n[2],done_n[2],reject_n[2],fail_n[2];

static void handler(int s){(void)s;stop=1;}
static void pause_ms(void){struct timespec t={0,20000000};nanosleep(&t,NULL);}
static int ix(unsigned c){return c==STD?0:c==PRI?1:-1;}
static const char *cn(int i){return i?"PRIORITY":"STANDARD";}
static void rmq(int *q){if(*q>=0){msgctl(*q,IPC_RMID,NULL);*q=-1;}}
static void reply(int q,unsigned long long id,int status,const char *msg){
 Response r={0};r.type=MT;r.magic=MAGIC;r.id=id;r.status=status;snprintf(r.text,sizeof r.text,"%s",msg);
 if(msgsnd(q,&r,sizeof r-sizeof(long),IPC_NOWAIT)<0)fprintf(stderr,"[router] reply delivery failed: %s\n",strerror(errno));
}
static void worker(int i){
 signal(SIGTERM,SIG_DFL);signal(SIGINT,SIG_DFL);Work w;
 printf("[worker:%s] online pid=%ld queue=%d\n",cn(i),(long)getpid(),wq[i]);fflush(stdout);
 for(;;){
  ssize_t n=msgrcv(wq[i],&w,sizeof w-sizeof(long),MT,0);
  if(n<0){if(errno==EINTR)continue;if(errno==EIDRM||errno==EINVAL)_exit(0);_exit(1);}
  if((size_t)n!=sizeof w-sizeof(long)||w.magic!=MAGIC||!w.len||w.len>MAX)continue;
  w.data[MAX-1]=0;
  if(!strcmp(w.data,"CRASH")){fprintf(stderr,"[worker:%s] simulated unexpected exit\n",cn(i));_exit(42);}
  Response r={0};r.type=MT;r.magic=MAGIC;r.id=w.id;r.reply=w.reply;r.cls=i+1;
  snprintf(r.text,sizeof r.text,"%s worker processed: %.220s",cn(i),w.data);
  if(msgsnd(sq,&r,sizeof r-sizeof(long),IPC_NOWAIT)<0&&(errno==EIDRM||errno==EINVAL))_exit(0);
 }
}
static void cleanup(void){
 for(int i=0;i<2;i++)if(wp[i]>0)kill(wp[i],SIGTERM);
 for(int i=0;i<2;i++)if(wp[i]>0)waitpid(wp[i],NULL,0);
 rmq(&rq);rmq(&wq[0]);rmq(&wq[1]);rmq(&sq);
}
static int router(void){
 signal(SIGINT,handler);signal(SIGTERM,handler);
 rq=msgget(KEY,IPC_CREAT|IPC_EXCL|0600);
 if(rq<0){if(errno==EEXIST)fprintf(stderr,"Router queue exists; run ./mq_router cleanup if no router is active.\n");else perror("msgget");return 1;}
 wq[0]=msgget(IPC_PRIVATE,IPC_CREAT|0600);wq[1]=msgget(IPC_PRIVATE,IPC_CREAT|0600);sq=msgget(IPC_PRIVATE,IPC_CREAT|0600);
 if(wq[0]<0||wq[1]<0||sq<0){perror("msgget private queue");cleanup();return 1;}
 for(int i=0;i<2;i++){wp[i]=fork();if(wp[i]<0){perror("fork");stop=1;break;}if(!wp[i])worker(i);alive[i]=1;}
 printf("[router] running pid=%ld request_queue=%d\n",(long)getpid(),rq);puts("[router] Ctrl+C stops; payload CRASH simulates worker failure");fflush(stdout);
 while(!stop){
  int st;pid_t p;while((p=waitpid(-1,&st,WNOHANG))>0)for(int i=0;i<2;i++)if(wp[i]==p){
   wp[i]=-1;alive[i]=0;fprintf(stderr,"[router] ALERT: %s worker exited (status=%d); class disabled\n",cn(i),WIFEXITED(st)?WEXITSTATUS(st):-1);
  }
  Request a={0};ssize_t n=msgrcv(rq,&a,sizeof a-sizeof(long)+1,MT,IPC_NOWAIT|MSG_NOERROR);
  if(n>=0){
   int i=n==(ssize_t)(sizeof a-sizeof(long))?ix(a.cls):-1;
   if(i>=0)recv_n[i]++;
   if((size_t)n!=sizeof a-sizeof(long)||a.magic!=MAGIC||a.version!=1||i<0||!a.id||a.reply<0||!a.len||a.len>MAX||a.data[a.len<=MAX?a.len-1:0]!=0){
    if(i>=0)reject_n[i]++;fprintf(stderr,"[router] malformed request rejected\n");
    if(n==(ssize_t)(sizeof a-sizeof(long))&&a.reply>=0&&a.id)reply(a.reply,a.id,2,"REJECTED: malformed request");
   }else if(!alive[i]){fail_n[i]++;reply(a.reply,a.id,3,"WORKER_UNAVAILABLE: selected worker exited");}
   else{
    Work w={0};w.type=MT;w.magic=MAGIC;w.id=a.id;w.reply=a.reply;w.len=a.len;memcpy(w.data,a.data,a.len);
    if(msgsnd(wq[i],&w,sizeof w-sizeof(long),IPC_NOWAIT)<0){
     fail_n[i]++;const char *e=errno==EAGAIN?"QUEUE_FULL: worker queue is full":(errno==EIDRM||errno==EINVAL)?"QUEUE_REMOVED: worker queue unavailable":"ROUTING_ERROR";
     reply(a.reply,a.id,4,e);fprintf(stderr,"[router] %s (%s)\n",e,strerror(errno));
    }else{sent_n[i]++;printf("[router] request %llu -> %s\n",a.id,cn(i));}
   }
  }else if(errno!=ENOMSG&&errno!=EINTR&&errno!=EIDRM)perror("msgrcv request");
  Response r;
  while((n=msgrcv(sq,&r,sizeof r-sizeof(long),MT,IPC_NOWAIT))>=0){
   int i=ix(r.cls);
   if((size_t)n!=sizeof r-sizeof(long)||r.magic!=MAGIC||!r.id||r.reply<0)continue;
   if(msgsnd(r.reply,&r,sizeof r-sizeof(long),IPC_NOWAIT)<0)fprintf(stderr,"[router] client reply queue unavailable\n");
   else{if(i>=0)done_n[i]++;printf("[router] response delivered id=%llu\n",r.id);}
  }
  if(n<0&&errno!=ENOMSG&&errno!=EINTR&&errno!=EIDRM)perror("msgrcv response");
  pause_ms();
 }
 puts("\n========== ROUTER STATISTICS ==========");
 for(int i=0;i<2;i++)printf("%-9s received=%lu forwarded=%lu completed=%lu rejected=%lu failed=%lu worker_alive=%s\n",cn(i),recv_n[i],sent_n[i],done_n[i],reject_n[i],fail_n[i],alive[i]?"yes":"no");
 cleanup();puts("[router] stopped; owned queues removed.");return 0;
}
static int client(int ac,char **av){
 if(ac<4){fprintf(stderr,"Usage: %s client <standard|priority> <message>\n",av[0]);return 2;}
 unsigned c=!strcmp(av[2],"standard")?STD:!strcmp(av[2],"priority")?PRI:0;
 size_t len=strlen(av[3]);if(!c||!len||len>=MAX){fprintf(stderr,"Choose standard/priority and a message of 1-%d bytes.\n",MAX-1);return 2;}
 int q=msgget(KEY,0600);if(q<0){perror("Router unavailable");return 1;}
 int own=msgget(IPC_PRIVATE,IPC_CREAT|0600);if(own<0){perror("private reply queue");return 1;}
 unsigned long long id=((unsigned long long)(unsigned)getpid()<<32)^(unsigned long long)time(NULL)^(unsigned long long)clock();if(!id)id=1;
 Request a={0};a.type=MT;a.magic=MAGIC;a.version=1;a.cls=c;a.id=id;a.reply=own;a.len=(unsigned)len+1;memcpy(a.data,av[3],len+1);
 if(msgsnd(q,&a,sizeof a-sizeof(long),IPC_NOWAIT)<0){perror("msgsnd");msgctl(own,IPC_RMID,NULL);return 1;}
 printf("[client:%s] sent request_id=%llu\n",cn((int)c-1),id);time_t start=time(NULL);Response r;
 while(time(NULL)-start<TIMEOUT){
  ssize_t n=msgrcv(own,&r,sizeof r-sizeof(long),MT,IPC_NOWAIT);
  if(n>=0&&(size_t)n==sizeof r-sizeof(long)&&r.magic==MAGIC&&r.id==id){printf("[client] response id=%llu status=%d: %s\n",r.id,r.status,r.text);msgctl(own,IPC_RMID,NULL);return r.status?1:0;}
  if(n<0&&errno!=ENOMSG&&errno!=EINTR){perror("msgrcv");break;}pause_ms();
 }
 fprintf(stderr,"[client] TIMEOUT waiting for request_id=%llu\n",id);msgctl(own,IPC_RMID,NULL);return 1;
}
static int clean(void){int q=msgget(KEY,0600);if(q<0){if(errno==ENOENT){puts("No router queue to clean.");return 0;}perror("msgget");return 1;}if(msgctl(q,IPC_RMID,NULL)<0){perror("IPC_RMID");return 1;}puts("Removed router request queue; stop any active router before restarting.");return 0;}
int main(int ac,char **av){
 if(ac<2){fprintf(stderr,"Usage: %s router | client <standard|priority> <message> | cleanup\n",av[0]);return 2;}
 if(!strcmp(av[1],"router"))return router();
 if(!strcmp(av[1],"client"))return client(ac,av);
 if(!strcmp(av[1],"cleanup"))return clean();
 fprintf(stderr,"Unknown command: %s\n",av[1]);return 2;
}
