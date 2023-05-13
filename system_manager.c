/*Trabalho Elaborado por:
Joel Oliveira 2021215037
João Querido  2021240660
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <semaphore.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <stdbool.h>
#include <sys/msg.h>

#define SENSOR_PIPE "SENSOR_PIPE"
#define CONSOLE_PIPE "CONSOLE_PIPE"
#define LOG_FILE "log.txt"
//#define DEBUG

typedef struct INTERNAL_QUEUE{
    char msg[125];
    short int priority;
    struct INTERNAL_QUEUE *next;
}INTERNAL_QUEUE;

typedef struct INTERNAL_QUEUE_HEAD{
    INTERNAL_QUEUE *start;
    INTERNAL_QUEUE *end;
}INTERNAL_QUEUE_HEAD;
typedef struct sensors_list{
    char id[33];
}sensors_list;
typedef struct sensor_struct{
    bool alerted;//so the alert watcher can know if it already alerted that value
    char key[33];
    int last_value,min,max,updates;
    double average;
}sensor_struct;
typedef struct alert_struct{
    int min,max,user_console_id;
    char id[33];
    char key[33];
}alert_struct;
typedef struct{
    int sensor_stats_count,sensor_count,alerts_count;
    bool *workers_array;//if 0 is free otherwise is working
    sensor_struct *sensor_stats_array;
    sensors_list *sensors_array;
    alert_struct *alerts_array;
} mem_struct;

typedef struct {
long mtype;
char content[89];//id (max 32 char)+5*10 (max int = 10 char)+ 5 spaces
}MSG;

int console_fd,sensor_fd,cur_queue_sz,shmid,msqid;
/// @brief QUEUE_SZ, N_WORKERS, MAX_KEYS, MAX_SENSORS, MAX_ALERTS
int config[5],global_channel,worker_global_n;
mem_struct* mem;
sem_t *available_workers_sem,*shm_mutex,*print_mutex,*sensor_stats_mutex;
pid_t main_process_id,alerts_watcher_process_id;
pthread_t sensor_reader,console_reader,dispatcher;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t queue_full_cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t queue_empty_cond = PTHREAD_COND_INITIALIZER;
FILE *log_file,*config_file;


INTERNAL_QUEUE_HEAD *head;
/// @brief 
/// @param function 0 prints in log_file and in stdout, 1 prints only in log_file, 2 prints only in stdout
/// @param s 
void print(short int function,char s[]){
    sem_wait(print_mutex);
    if(function == 0 || function == 2){
        printf("%s\n",s);
    }
    if(function == 0 || function == 1){
        time_t t;
        time(&t);
        char *c=ctime(&t);
        c[strlen(c)-1]='\0';//deletes '\n'
        fprintf(log_file,"%s %s\n",c,s);
        fflush(log_file);
    }
    sem_post(print_mutex);
}
void cleaner(){
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_full_cond);
    pthread_cond_destroy(&queue_empty_cond);
    sem_close(available_workers_sem);
    sem_close(shm_mutex);
    sem_close(sensor_stats_mutex);
    shmdt(mem);
    signal(SIGINT,SIG_IGN);
    fclose(config_file);
    fclose(log_file);
    sem_close(print_mutex);
    exit(0);
}
void handler(int signo){
    pthread_t current_thread = pthread_self();
    pid_t current_process = getpid();
    int return_value;
    if(signo==SIGINT){
        if(current_thread != console_reader && current_thread != sensor_reader && current_thread != dispatcher){//then it is the main thread
            print(0,"SIGINT recived");
            print(0,"HOME_IOT is going to close");
            return_value = pthread_cancel(console_reader);
            if(return_value!=0){
                print(0,strerror(return_value));
            }
            return_value = pthread_cancel(sensor_reader);
            if(return_value!=0){
                print(0,strerror(return_value));
            }
            return_value = pthread_cancel(dispatcher);
            if(return_value!=0){
                print(0,strerror(return_value));
            }
            print(0,"All threads closed");
        }
    }else{
        if (current_process!=alerts_watcher_process_id){
            char s[42];
            sem_wait(shm_mutex);
            if(!*(mem->workers_array+worker_global_n)){//if worker is not working, then it can already exit
                sprintf(s,"Worker %d exiting",worker_global_n);
                print(0,s);
                return_value = close(global_channel);
                if(return_value==-1){
                    print(0,strerror(errno));
                }
                sem_post(shm_mutex);
            }else{ //other wise when it finishes is job it will read EOF because the unnamed pipe was closed on the father side
                sem_post(shm_mutex);
                sprintf(s,"Waiting for worker %d to finish",worker_global_n);
                print(0,s);
                return;
            }
        }else{//if it is alert_watcher process
            print(0,"alerts_watcher exiting");
        }
        cleaner();
    }
}
void add_to_queue(short int priority,char content[]){
    INTERNAL_QUEUE *ptr,*aux_ptr,*pre_aux_ptr;

    pthread_mutex_lock(&queue_mutex);
    if(cur_queue_sz < config[0] || priority > 0){// if the queue is not full or if it has priority
        while(cur_queue_sz >= config[0]){//if the queue is full but the content has priority it waits unitl the queue has some space left
            #ifdef DEBUG
            print(2,"Priority content waiting for queue to free 1 space");
            #endif
            pthread_cond_wait(&queue_full_cond,&queue_mutex);
            #ifdef DEBUG
            print(2,"Priority content waited and now will procced");
            #endif
        }
        ptr = (INTERNAL_QUEUE*)malloc(sizeof(INTERNAL_QUEUE));
        strcpy(ptr->msg,content);
        ptr->priority = priority;
        ptr->next = NULL;

        if (head->start == NULL){//if the queue is empty
            head->start = ptr;
            head->end = ptr;
        }else if(!priority){//if it isn't empty and doesn't have priority it goes to the end
            head->end->next = ptr;
            head->end = ptr;
        }else{
            aux_ptr = head->start;
            pre_aux_ptr = head->start;
            while(aux_ptr->priority > 0){
                pre_aux_ptr = aux_ptr;
                aux_ptr = aux_ptr->next;
            }
            if(aux_ptr != pre_aux_ptr){
                pre_aux_ptr->next = ptr;
                ptr->next = aux_ptr;
            }else{
                head->start = ptr;
                ptr->next = aux_ptr;
            }
        }
        cur_queue_sz++;

    }else{
        char temp[123];
        sprintf(temp,"QUEUE IS FULL:%s discarted",content);
        print(0,temp);
        }
    pthread_cond_signal(&queue_empty_cond);
    pthread_mutex_unlock(&queue_mutex);
}

/// @brief pops the first element of the queue
short int pop_queue(char dest_string[]){
    INTERNAL_QUEUE *ptr;
    ptr = head->start;
    if (ptr == NULL)return 0;//nothing to pop
    head->start = ptr->next;
    strcpy(dest_string,ptr->msg);
    if(head->start == NULL)head->end = NULL;//if the queue is now empty
    cur_queue_sz--;
    free(ptr);
    return 1;
} 
void work_sensor(char s[]){
    char *id,*key,*value;//tokens
    char car[] = "#";
    int i,val;
    bool found = false;
    id = strtok(s,car);
    
    for(i=0;i<mem->sensor_count;i++){
        if(strcmp(id,(mem->sensors_array+i)->id)==0){//if id was found
            found = true;
            break;
        }
    }
    
    if(!found){//if it was not found
        if(mem->sensor_count<=config[3]){//if sensor list is not full
            strcpy((mem->sensors_array+i)->id,id);
            mem->sensor_count++;
        }else{
            print(0,"Sensors list is Full!");
            return;
            }
    }
    key = strtok(0,car);
    value = strtok(0,car);
    
    found = false;
    for (i = 0; i < mem->sensor_stats_count; i++){
        if(strcmp(key,(mem->sensor_stats_array+i)->key)==0){
            found = true;
            break;
        }
    }
    
    val = atoi(value);
    sensor_struct *ss = mem->sensor_stats_array+i;
    
    if(found){//if the key was found
        ss->average = (ss->updates * ss->average + val)/(ss->updates + 1);
        ss->last_value = val;
        ss->updates++;
        if(val > ss->max)ss->max = val;
        else if(val < ss->min)ss->min = val;
        ss->alerted = false;
    }else{//if not found
        if(mem->sensor_stats_count <= config[2]){
            strcpy(ss->key,key);
            ss->average = ss->last_value = ss->max = ss->min = val;
            ss->updates = 1;
            mem->sensor_stats_count++;
            ss->alerted = false;
        }else{
            print(0,"Stats array is full!");
            return;
        }
    }
    sem_post(sensor_stats_mutex);
}
void error_sending(char id[]){
    char temp[300];
    sprintf(temp,"Error sending message to %s: %s",id,strerror(errno));
    sem_post(print_mutex);
    print(0,temp);
    sem_wait(print_mutex);
}
void stats(char *id,MSG msg){
    sensor_struct *ss;
    #ifdef DEBUG
    sem_wait(print_mutex);
    printf("Key Last Min Max Avg Count\n");
    #endif
    strcpy(msg.content,"Key Last Min Max Avg Count");
    if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
        error_sending(id);
    }
    for(int i=0;i<config[2];i++){
        ss = mem->sensor_stats_array+i;
        if(i>= mem->sensor_stats_count)break;
        #ifdef DEBUG
        printf("%s %d %d %d %f %d\n",ss->key,ss->last_value,ss->min,ss->max,ss->average,ss->updates);
        #endif
        sprintf(msg.content,"%s %d %d %d %f %d",ss->key,ss->last_value,ss->min,ss->max,ss->average,ss->updates);
        if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
            error_sending(id);
        }
    }
    #ifdef DEBUG
    sem_post(print_mutex);
    #endif
}
void reset_stats(char *id,MSG msg){
    mem->sensor_count = 0;
    mem->sensor_stats_count = 0;
    #ifdef DEBUG
    print(2,"OK");
    #endif
    strcpy(msg.content,"OK");
    if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
        error_sending(id);
    }
}
void sensors(char *id,MSG msg){
    sensors_list *sl;
        #ifdef DEBUG
        sem_wait(print_mutex);
        printf("ID\n");
        #endif
        strcpy(msg.content,"ID");
        if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
            error_sending(id);
        }
        for (int i = 0; i < config[3]; i++){
            sl = mem->sensors_array+i;
            if(i>=mem->sensor_count)break;
            #ifdef DEBUG
            printf("%s\n",sl->id);
            #endif
            sprintf(msg.content,"%s",sl->id);
            if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
                error_sending(id);
            }
        }
        #ifdef DEBUG
        sem_post(print_mutex);
        #endif
}
void add_alert(char *id,MSG msg){
    char *s1;
    char car[]="#";
    alert_struct *alert_p = mem->alerts_array;
        s1 = strtok(0,car);
        for (int i = 0; i < mem->alerts_count; i++){
            if(strcmp((alert_p+i)->id,s1)==0){
                sprintf(msg.content,"ERROR: %s alert id already exists",s1);
                if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
                    error_sending(id);
                }
                return;
            }
            alert_p++;
        }
        // if not found and the number of alerts is within count then it can be added
        if(mem->alerts_count+1<=config[4]){
            strcpy(alert_p->id,s1);
            s1 = strtok(0,car);
            strcpy(alert_p->key,s1);
            s1 = strtok(0,car);
            alert_p->min = atoi(s1);
            s1 = strtok(0,car);
            alert_p->max = atoi(s1);
            alert_p->user_console_id = atoi(id);
            strcpy(msg.content,"OK");
            mem->alerts_count++;
            if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
                error_sending(id);
            }
        }else{
            sprintf(msg.content,"ERROR: %s: alerts list is full",s1);
            if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
                error_sending(id);
            }
            return;
        }
}
void remove_alert(char *id,char *s1,MSG msg){
    alert_struct *alert_p = mem->alerts_array;
    int i;
    for (i = 0; i < mem->alerts_count; i++){
        if(strcmp((alert_p+i)->id,s1)==0){
            *(alert_p+i) = *(alert_p+(mem->alerts_count-1));//copies the last alert on the list to the place where the alert was removed
            mem->alerts_count--;
            strcpy(msg.content,"OK");
            if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
                error_sending(id);
            }
            return;
        }
    }
    sprintf(msg.content,"ERRO: %s alert id doesn't exists",s1);
    if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
        error_sending(id);
    }
}
void list_alerts(char *id,MSG msg){
    alert_struct *alert_p = mem->alerts_array;
    strcpy(msg.content,"ID Key MIN MAX");
    if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
        error_sending(id);
    }
    for (int i = 0; i < mem->alerts_count; i++){
        sprintf(msg.content,"%s %s %d %d",alert_p->id,alert_p->key,alert_p->min,alert_p->max);
        if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
            error_sending(id);
        }
        alert_p++; 
    }
}
void work_user(char s[]){
    char *id,*s1;
    char car[]="#";
    MSG msg;
    id = strtok(s,car);
    msg.mtype = atol(id);
    s1 = strtok(0,car);
    if(strcmp(s1,"1")==0){
        stats(id,msg);
    }else if(strcmp(s1,"2")==0){
        reset_stats(id,msg);
    }else if(strcmp(s1,"3")==0){
        sensors(id,msg);
    }else if(strcmp(s1,"4")==0){
        add_alert(id,msg);
    }else if(strcmp(s1,"5")==0){
        s1 = strtok(0,car);//alert id
        remove_alert(id,s1,msg);
    }else if(strcmp(s1,"6")==0){
        list_alerts(id,msg);
    }else{
        char temp[200];
        sprintf(temp,"Error: BAD COMMAND: %s\n",s);
    }
}
void work(int channel[]){
    close(channel[1]);
    ssize_t size;
    char s[123];
    char temp[200];
    
    sprintf(s,"WORKER %d READY",worker_global_n);
    print(0,s);
    while (1){
        size = read(channel[0],s,sizeof(s));
        if(size == 0){
            handler(SIGCHLD);
        }else if(size == -1){
            char temp[300];
            sprintf(temp,"Error reading on worker %d unamed_pipe: %s",worker_global_n,strerror(errno));
            print(0,temp);
        }
        s[size]=0;//indicates end of line
        sem_post(shm_mutex);
        if(s[strlen(s)-1]=='S'){//Dados de um sensor
            #ifdef DEBUG
            print(2,"Worker a registar dados de sensor");
            #endif
            sem_wait(shm_mutex);
            //sleep(worker_global_n+3);//it is not for sincronization, its only to simulate a longer work
            work_sensor(s);
            sem_post(shm_mutex);
        }else{//Dados de um user
            #ifdef DEBUG
            print(2,"User stuff\n");
            #endif
            sem_wait(shm_mutex);
            //sleep(worker_global_n+3);//it is not for sincronization, its only to simulate a longer work
            work_user(s);
            sem_post(shm_mutex);
        }

        sprintf(temp,"WORKER %d:%s PROCESSING COMPLETE",worker_global_n,s);
        print(0,temp);
        sem_wait(shm_mutex);
        *(mem->workers_array+worker_global_n) = false;
        sem_post(shm_mutex);
        sem_post(available_workers_sem);
    }
}
void alerts_watcher(){
    print(0,"Alert Watcher created!");
    alert_struct *alert;
    sensor_struct *sensor_stats;
    while (1){
        sem_wait(sensor_stats_mutex);
        sem_wait(shm_mutex);
        alert = mem->alerts_array;
        for (int i = 0; i < mem->alerts_count; i++){
            sensor_stats = mem->sensor_stats_array;
            for (int j = 0; j < mem->sensor_stats_count; j++){
                if(!sensor_stats->alerted){
                    if(strcmp(alert->key,sensor_stats->key)==0){
                        if(sensor_stats->last_value<alert->min || sensor_stats->last_value>alert->max){
                            MSG msg;
                            msg.mtype = alert->user_console_id;
                            sprintf(msg.content,"ALERT %s TRIGERED: %d",alert->id,sensor_stats->last_value);
                            if(msgsnd(msqid,&msg,sizeof(msg)-sizeof(long),0)==-1){
                                char temp[10];
                                sprintf(temp,"%d",alert->user_console_id);
                                error_sending(temp);
                            }
                            print(0,msg.content);
                        }
                    }
                }
                sensor_stats++;
            }
            alert++;
        }
        sensor_stats = mem->sensor_stats_array;
        for (int i = 0; i < mem->sensor_stats_count; i++){
            sensor_stats->alerted = true;
            sensor_stats++;
        }
        
        sem_post(shm_mutex);
    }
    
}
void *console(){
    // Opens the CONSOLE_PIPE for reading;

    char s[125];
    print(0,"Console thread starting!");
    while (1){
        if ((console_fd = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
            sprintf(s,"Cannot open CONSOLE_PIPE for reading: %s",strerror(errno));
            print(0,s);
            exit(0);
        }
        #ifdef DEBUG
        print(2,"CONSOLE_PIPE opened!");
        #endif
        int read_return;
        
        while (1){
            read_return = read(console_fd,s,sizeof(s));
            if(read_return > 0){
                #ifdef DEBUG
                sem_wait(print_mutex);
                printf("Recived from CONSOLE_PIPE: %s\n",s);
                sem_post(print_mutex);
                #endif
                strcat(s,"#C");
                add_to_queue(1,s);
            }else if (read_return == -1){
                sprintf(s,"Error reading from CONSOLE_PIPE: %s",strerror(errno));
                print(0,s);
            }else{
                close(console_fd);
                print(2,"CONSOLE_PIPE closed: There are no more pipes opened on the other end");
                break;
            }
        }
    }
    return NULL;
    
}
void *sensor(){
    // Opens the SENSOR_PIPE for reading

    char s[125];
    print(0,"Sensor thread starting!");
    while (1){
        if ((sensor_fd = open(SENSOR_PIPE, O_RDONLY)) < 0) {
            sprintf(s,"Cannot open SENSOR_PIPE for reading: %s",strerror(errno));
            print(0,s);
            exit(0);
        }
        #ifdef DEBUG
        print(2,"SENSOR_PIPE opened!");
        #endif
        int read_return;
        
        while (1){
            read_return = read(sensor_fd,s,sizeof(s));
            
            if(read_return > 0){
                #ifdef DEBUG
                sem_wait(print_mutex);
                printf("Recived from SENSOR_PIPE: %s\n",s);
                sem_post(print_mutex);
                #endif
                strcat(s,"#S");
                add_to_queue(0,s);
            }else if (read_return == -1){
                sprintf(s,"Error reading from SENSOR_PIPE: %s",strerror(errno));
                print(0,s);
            }else{
                close(sensor_fd);
                print(2,"SENSOR_PIPE closed: There aren't any connections on the other end");
                break;
            }
        }
    }
    return NULL;
}
void *dispatch(void *arg){
    int (*channels)[2] = arg;
    char s[123];
    char temp[200];
    print(0,"Dispatch thread starting!");
    
    while (1){
        pthread_mutex_lock(&queue_mutex);
        while (cur_queue_sz < 1){
            #ifdef DEBUG
            print(2,"QUEUE is empty! Nothing to pop!");
            #endif
            pthread_cond_wait(&queue_empty_cond,&queue_mutex);
        }
        pthread_mutex_unlock(&queue_mutex);
        sem_wait(available_workers_sem);
        for (int i = 0; i < config[1]; i++){
            sem_wait(shm_mutex);//because of workers_array
            if(!*(mem->workers_array+i)){//if worker not occupied
                pthread_mutex_lock(&queue_mutex);
                pop_queue(s);
                pthread_cond_signal(&queue_full_cond);
                pthread_mutex_unlock(&queue_mutex);
                #ifdef DEBUG
                sprintf(temp,"Removed from queue: %s",s);
                print(2,temp);
                #endif
                *(mem->workers_array+i)=true;
                sem_post(shm_mutex);
                sprintf(temp,"DISPATCHER: %s SENT FOR PROCESSING ON WORKER %d",s,i);
                print(0,temp);
                write(channels[i][1],s,strlen(s)+1);
                break;
            }
            sem_post(shm_mutex);
            #ifdef DEBUG
            sprintf(temp,"Worker %d is occupied!",i);
            print(2,temp);
            #endif
        }
    }
    //waitpid(-1,NULL,0);
    return NULL;
}
void startup_error_cleaner(int stage,char s[]){
    time_t t;
    time(&t);
    char *c=ctime(&t);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_full_cond);
    pthread_cond_destroy(&queue_empty_cond);
    if(stage==0){
        printf("Erro, Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n");
        fprintf(log_file,"%s Erro, Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n",c);
    }
    if(stage==1){
        perror("Config_file error");
        fprintf(log_file,"%s Config_file error: %s",c,strerror(errno));
    }
    if(stage>=2){
        if(stage==2){
            printf("Parâmetros insuficientes no ficheiro de configurações!\n");
            fprintf(log_file,"%s Parâmetros insuficientes no ficheiro de configurações!\n",c);
        }
        fclose(config_file);
        if(stage==3){
            printf("Existe um ou mais parâmetros incorretos!\n");
            fprintf(log_file,"%s Existe um ou mais parâmetros incorretos!\n",c);
        }
        if(stage>3){
            perror(s);
            fprintf(log_file,"%s %s: %s\n",c,s,strerror(errno));
            if(stage>4){
                shmctl(shmid, IPC_RMID, NULL);
                if(stage>5){
                    shmdt(mem);
                    if(stage>6){
                        msgctl(msqid, IPC_RMID, NULL);
                        if(stage>7){
                            sem_close(available_workers_sem);
                            unlink("AVAILABLE_WORKERS");
                            if(stage>8){
                                sem_close(shm_mutex);
                                unlink("SHM_MUTEX");
                                if(stage>9){
                                    sem_close(print_mutex);
                                    unlink("PRINT_MUTEX");
                                    if(stage>10){
                                        sem_close(sensor_stats_mutex);
                                        unlink("SENSOR_STATS_MUTEX");
                                        if(stage>11){
                                            close(console_fd);
                                            unlink(CONSOLE_PIPE);
                                            if(stage>12){
                                                close(sensor_fd);
                                                unlink(SENSOR_PIPE);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    

    fflush(log_file);
    fclose(log_file);
    exit(0);
}
int main(int argc, char const *argv[]){
    signal(SIGINT,SIG_IGN);//ignores ctrl c
    signal(SIGTERM,SIG_IGN);
    log_file = fopen(LOG_FILE,"w");
    if(log_file == NULL){
        perror("log_file error");
        exit(0);
    }
    time_t t;
    time(&t);
    char *c=ctime(&t);
    c[strlen(c)-1]='\0';//deletes '\n'
    printf("HOME_IOT SIMULATOR STARTING\n");
    fprintf(log_file,"%s HOME_IOT SIMULATOR STARTING\n",c);
    fflush(log_file);
    if (argc < 2){
        startup_error_cleaner(0,"");
    }
    if(access(argv[1],R_OK)==0){//tests if the file exists and if it has read permissions
        config_file = fopen(argv[1],"r");
    }else{
        startup_error_cleaner(1,"");
    }
   
    char s[11];
    for (int i = 0; i < 5; i++){
        if(fgets(s,11,config_file)==NULL){
            startup_error_cleaner(2,"");
        }
        config[i]=atoi(s);
        if (config[i] < 1){//the last element of the config file (MAX_ALERTS) can be 0
            time(&t);
            char *c=ctime(&t);
            c[strlen(c)-1]='\0';//deletes '\n'
            if (i<4 || config[i]<0){//but cannot be less than 0
                startup_error_cleaner(3,"");
            }
        }
    }
    shmid = shmget(IPC_PRIVATE, sizeof(mem_struct)+config[1]*sizeof(bool)+config[2]*sizeof(sensor_struct)+config[3]*sizeof(sensors_list)+config[4]*sizeof(alert_struct), IPC_CREAT|0700);
    if (shmid < 1)startup_error_cleaner(4,"Erro no shmget");

    #ifdef DEBUG
    printf("Shared memory created!\n");
    #endif

    mem = (mem_struct*) shmat(shmid, NULL, 0);
    if (mem < (mem_struct*) 1)startup_error_cleaner(5,"Erro no shmat");
    
    #ifdef DEBUG
    printf("Shared memory attached!\n");
    #endif

    mem->workers_array = (bool*) (mem+1);
    mem->sensor_stats_array = (sensor_struct*)(mem->workers_array + config[1]);
    mem->sensors_array = (sensors_list*)(mem->sensor_stats_array + config[2]);
    mem->alerts_array = (alert_struct*)(mem->sensors_array + config[3]);

    mem->sensor_stats_count = mem->sensor_count = mem->alerts_count = 0;

    key_t key = ftok("user_console.c",'B');
    if (key==-1){
        startup_error_cleaner(6,"Error on ftok key generation");
    }
    
    msqid = msgget(key,IPC_CREAT|0700);
    if(msqid == -1){
        startup_error_cleaner(6,"Error Cannot get message queue");
    }

    sem_unlink("AVAILABLE_WORKERS");
	available_workers_sem = sem_open("AVAILABLE_WORKERS",O_CREAT|O_EXCL,0700,config[1]);
    if(available_workers_sem==SEM_FAILED)startup_error_cleaner(7,"Erro na abertura do semaforo AVAILABLE_WORKERS");
	sem_unlink("SHM_MUTEX");
	shm_mutex =sem_open("SHM_MUTEX",O_CREAT|O_EXCL,0700,1);
    if(available_workers_sem==SEM_FAILED)startup_error_cleaner(8,"Erro na abertura do semaforo SHM_MUTEX");
	sem_unlink("PRINT_MUTEX");
	print_mutex =sem_open("PRINT_MUTEX",O_CREAT|O_EXCL,0700,1);
    if(available_workers_sem==SEM_FAILED)startup_error_cleaner(9,"Erro na abertura do semaforo PRINT_MUTEX");
    sem_unlink("SENSOR_STATS_MUTEX");
	sensor_stats_mutex =sem_open("SENSOR_STATS_MUTEX",O_CREAT|O_EXCL,0700,1);
    if(available_workers_sem==SEM_FAILED)startup_error_cleaner(10,"Erro na abertura do semaforo SENSOR_STATS_MUTEX");

    struct sigaction new;
    new.sa_handler = handler;
    new.sa_flags = 0;
    sigfillset(&new.sa_mask);

    int channels[config[1]][2];
    __pid_t pid;
    main_process_id = getpid();

    for (int i = 0; i < config[1]; i++){
        pipe(channels[i]);
        pid = fork();
        if(pid == 0){//if it is the son
            sigaction(SIGTERM,&new,NULL);
            worker_global_n = i;
            global_channel = channels[i][0];
            print_mutex = sem_open("PRINT_MUTEX",0);
            shm_mutex = sem_open("SHM_MUTEX",0);
            available_workers_sem = sem_open("AVAILABLE_WORKERS",0);
            work(channels[i]);
            exit(0);
        }else if(pid ==-1){
            for (int j = 0; j < i+1; j++){
                close(channels[j][1]);//close the unnamed pipes from the father side
            }
            kill(0,SIGTERM);//sends a kill signal to workers and to alerts_watcher
            while(waitpid((pid_t)-1,NULL,0)>0);//waits for the workers to finish their job
            startup_error_cleaner(11,"Erro worker creation");
        }
        close(channels[i][0]);
    }
    alerts_watcher_process_id = fork();
    if(alerts_watcher_process_id==0){//if it is the son
        alerts_watcher_process_id = getpid();
        sigaction(SIGTERM,&new,NULL);
        alerts_watcher();
        exit(0);
    }else if(alerts_watcher_process_id ==-1){
        char temp_s[100];
        sprintf(temp_s,"Erro alert_watcher creation: %s",strerror(errno));
        print(0,temp_s);
        kill(0,SIGKILL);
    }
    unlink(CONSOLE_PIPE);
    if ((mkfifo(CONSOLE_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        startup_error_cleaner(11,"Cannot create CONSOLE_PIPE");
    }
    unlink(SENSOR_PIPE);
    if ((mkfifo(SENSOR_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        startup_error_cleaner(12,"Cannot create SENSOR_PIPE");
    }
    
    cur_queue_sz = 0;

    head = (INTERNAL_QUEUE_HEAD*)malloc(sizeof(INTERNAL_QUEUE_HEAD));
    if(head==NULL)startup_error_cleaner(13,"Erro no malloc");
    head->start = NULL;
    head->end = NULL;
    
    sigaction(SIGINT,&new,NULL);

    int return_value;
    INTERNAL_QUEUE *temp;
    return_value = pthread_create(&console_reader,NULL,console,NULL);
    if(return_value != 0){
        errno=return_value;
        startup_error_cleaner(13,"Erro na criação da thread CONSOLE_READER");
    }
    return_value=pthread_create(&sensor_reader,NULL,sensor,NULL);
    if(return_value != 0){
        errno=return_value;
        startup_error_cleaner(13,"Erro na criação da thread SENSOR_READER");
    }
    return_value=pthread_create(&dispatcher,NULL,dispatch,channels);
    if(return_value != 0){
        errno=return_value;
        startup_error_cleaner(13,"Erro na criação da thread DISPATCHER");
    }
    pthread_join(console_reader,NULL);
    pthread_join(sensor_reader,NULL);
    pthread_join(dispatcher,NULL);
    for (int i = 0; i < config[1]; i++){
        close(channels[i][1]);//close the unnamed pipes from the father side
    }
    kill(0,SIGTERM);//sends a kill signal to workers and to alerts_watcher
    close(console_fd);
    unlink(CONSOLE_PIPE);
    close(sensor_fd);
    unlink(SENSOR_PIPE);
    while(waitpid((pid_t)-1,NULL,0)>0);//waits for the workers to finish their job, it may also wait for alerts_watcher
    sem_unlink("AVAILABLE_WORKERS");
    sem_unlink("SHM_MUTEX");
    sem_unlink("PRINT_MUTEX");
    sem_unlink("SENSOR_STATS_MUTEX");
    msgctl(msqid, IPC_RMID, NULL);
    shmctl(shmid, IPC_RMID, NULL);
    print(0,"Cleanig INTERNAL_QUEUE");
    while (head->start!=NULL){
        temp = head->start->next;
        print(0,head->start->msg);
        free(head->start);
        head->start = temp;
    }
    print(0,"INTERNAL QUEUE WAS CLEANED");
    print(0,"HOME_IOT IS EXITING");
    free(head);
    cleaner();
    return 0;
}
