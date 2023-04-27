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

#define SENSOR_PIPE "SENSOR_PIPE"
#define CONSOLE_PIPE "CONSOLE_PIPE"
#define LOG_FILE "log.txt"
#define DEBUG
#define N_WORKERS 5

typedef struct INTERNAL_QUEUE{
    char msg[123];
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
    //char **key;
    char key[33];
    int last_value,min,max,updates;
    double average;
}sensor_struct;
typedef struct alert_struct{
    char user_console_id[32];
    char key[32];
    int min,max;
}alert_struct;
typedef struct{
    int sensor_stats_count,sensor_count,alerts_count;
    bool *workers_array;//if 0 is free otherwise is working
    sensor_struct *sensor_stats_array;
    sensors_list *sensors_array;
    alert_struct *alerts_array;
} mem_struct;

//bool *workers_array;
int console_fd,sensor_fd,cur_queue_sz,shmid,channels[5][2];
/// @brief QUEUE_SZ, N_WORKERS, MAX_KEYS, MAX_SENSORS, MAX_ALERTS
int config[5];
mem_struct* mem;
sem_t *available_workers_sem,*shm_mutex,*print_mutex;
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
    if(function == 0 || function == 1){
        time_t t;
        time(&t);
        fprintf(log_file,"%s %s\n",ctime(&t),s);
    }
    if(function == 0 || function == 2){
        printf("%s",s);
    }
    sem_post(print_mutex);
}
void add_to_queue(short int priority,char content[]){
    INTERNAL_QUEUE *ptr,*aux_ptr,*pre_aux_ptr;

    pthread_mutex_lock(&queue_mutex);
    if(cur_queue_sz < config[0] || priority > 0){// if the queue is not full or if it has priority
        while(cur_queue_sz >= config[0]){//if the queue is full but the content has priority it waits unitl the queue has some space left
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("Priority content waiting for queue to free 1 space\n");
            sem_post(print_mutex);
            #endif
            pthread_cond_wait(&queue_full_cond,&queue_mutex);
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("Priority content waited and now will procced\n");
            sem_post(print_mutex);
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
        #ifdef DEBUG
        sem_wait(print_mutex);
        printf("QUEUE is FULL\n");
        sem_post(print_mutex);
        #endif
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
    
    for(i=0;i<config[3];i++){
        if(i>= mem->sensor_count)break;
        print(2,"H\n");
        if(strcmp(id,(mem->sensors_array+i)->id)==0){//if id was found
            found = true;
            break;
        }
    }
    
    if(!found){//if it was not found
        if(mem->sensor_count<config[3]){//if sensor list is not full
            strcpy((mem->sensors_array+i)->id,id);
            mem->sensor_count++;
        }else print(2,"Sensors list is Full\n");
    }
    key = strtok(0,car);
    value = strtok(0,car);
    
    found = false;
    for (i = 0; i < config[2]; i++){
        if(i >= mem->sensor_stats_count)break;
        print(2,"E\n");
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
    }else{//if not found
        if(mem->sensor_stats_count < config[2]){
            strcpy(ss->key,key);
            ss->average = ss->last_value = ss->max = ss->min = val;
            ss->updates = 1;
            mem->sensor_stats_count++;
        }else{
            print(2,"Stats array is full!\n");
        }
    }
}
void work_user(char s[]){
    char *s1;
    char car[]="#";
    s1 = strtok(s,car);
    s1 = strtok(0,car);
    if(strcmp(s1,"1")==0){
        sensor_struct *ss;
        sem_wait(print_mutex);
        printf("Key Last Min Max Avg Count\n");
        for(int i=0;i<config[2];i++){
            ss = mem->sensor_stats_array+i;
            if(i>= mem->sensor_stats_count)break;
            printf("%s %d %d %d %f %d\n",ss->key,ss->last_value,ss->min,ss->max,ss->average,ss->updates);
        }
        sem_post(print_mutex);
    }else if(strcmp(s1,"3")==0){
        sensors_list *sl;
        sem_wait(print_mutex);
        printf("ID\n");
        for (int i = 0; i < config[3]; i++){
            sl = mem->sensors_array+i;
            if(i>=mem->sensor_count)break;
            printf("%s\n",sl->id);
        }
        sem_post(print_mutex);
    }
}
void work(int worker_n){
    sem_wait(print_mutex);
    printf("worker %d starting\n",worker_n);
    sem_post(print_mutex);
    close(channels[worker_n][1]);
    char c;
    int i;
    ssize_t size;
    char s[123];
    while (1){
        size = read(channels[worker_n][0],s,sizeof(s));
        if(size == -1){
            time_t t = time(NULL);
            sem_wait(print_mutex);
            printf("Error reading from worker %d unamed_pipe: %s\n",worker_n,strerror(errno));
            fprintf(log_file,"%s Error reading from SENSOR_PIPE: %s\n",ctime(&t),strerror(errno));
            sem_post(print_mutex);
        }
        s[size]=0;//indicates end of line
        sem_post(shm_mutex);

        #ifdef DEBUG
        sem_wait(print_mutex);
        printf("WORKer %d : I've read: %s\n",worker_n,s);
        sem_post(print_mutex);
        #endif

        //sleep((rand()%(3-1+1))+1);
        i = 0;
        do{
            c = s[i];
            i++;
        } while (c!='#');
        i+=2;
        c = s[i];
        if(c!=0){//Dados de um sensor
            sem_wait(print_mutex);
            printf("Worker a registar dados de sensor\n");
            sem_post(print_mutex);
            sem_wait(shm_mutex);
            work_sensor(s);
            sem_post(shm_mutex);
        }else{//Dados de um user
            sem_wait(print_mutex);
            printf("User stuff\n");
            sem_post(print_mutex);
            sem_wait(shm_mutex);
            work_user(s);
            sem_post(shm_mutex);
        }
        sem_wait(shm_mutex);
        mem->workers_array[worker_n] = false;
        sem_post(shm_mutex);
        sem_post(available_workers_sem);
    }
}
void alerts_watcher(){
    sem_wait(print_mutex);
    printf("Alert Watcher created!\n");
    sem_post(print_mutex);
}
void *console(){
    // Opens the CONSOLE_PIPE for reading;
    time_t t;
    time(&t);
    sem_wait(print_mutex);
    printf("Console thread starting!\n");
    fprintf(log_file,"%s Console thread starting!\n",ctime(&t));
    sem_post(print_mutex);
    if ((console_fd = open(CONSOLE_PIPE, O_RDONLY)) < 0) {
        time(&t);
        sem_wait(print_mutex);
        perror("Cannot open CONSOLE_PIPE for reading");
        fprintf(log_file,"%s Cannot open CONSOLE_PIPE for reading: %s",ctime(&t),strerror(errno));
        sem_post(print_mutex);
        exit(0);
    }
    #ifdef DEBUG
    sem_wait(print_mutex);
    printf("CONSOLE_PIPE opened!\n");
    sem_post(print_mutex);
    #endif
    int read_return;
    char s[123];
    
    while (1){
        read_return = read(console_fd,s,sizeof(s));
        time(&t);
        if(read_return > 0){
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("Recived from CONSOLE_PIPE: %s\n",s);
            sem_post(print_mutex);
            #endif
            add_to_queue(1,s);
        }else if (read_return == -1){
            sem_wait(print_mutex);
            perror("Error reading from CONSOLE_PIPE\n");
            fprintf(log_file,"%s Error reading from CONSOLE_PIPE: %s\n",ctime(&t),strerror(errno));
            sem_post(print_mutex);
        }else printf("Console PIpe is empty\n");
    }
    return NULL;
    
}
void *sensor(){
    // Opens the SENSOR_PIPE for reading
    time_t t;
    time(&t);
    sem_wait(print_mutex);
    printf("Sensor thread starting!\n");
    fprintf(log_file,"%s Sensor thread starting!\n",ctime(&t));
    sem_post(print_mutex);
    if ((sensor_fd = open(SENSOR_PIPE, O_RDONLY)) < 0) {
        time(&t);
        sem_wait(print_mutex);
        perror("Cannot open SENSOR_PIPE for reading");
        fprintf(log_file,"%s Cannot open SENSOR_PIPE for reading: %s",ctime(&t),strerror(errno));
        sem_post(print_mutex);
        exit(0);
    }
    #ifdef DEBUG
    sem_wait(print_mutex);
    printf("SENSOR_PIPE opened!\n");
    sem_post(print_mutex);
    #endif
    int read_return;
    char s[77];
    
    while (1){
        read_return = read(sensor_fd,s,sizeof(s));
        time(&t);

        if(read_return > 0){
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("Recived from SENSOR_PIPE: %s\n",s);
            sem_post(print_mutex);
            #endif
            add_to_queue(0,s);
        }else if (read_return == -1){
            sem_wait(print_mutex);
            perror("Error reading from SENSOR_PIPE\n");
            fprintf(log_file,"%s Error reading from SENSOR_PIPE: %s\n",ctime(&t),strerror(errno));
            sem_post(print_mutex);
        }else printf("Sensor PIpe is empty\n");
    }
    return NULL;
}
void sigint_handler(){
    //INTERNAL_QUEUE *ptr;
    time_t t;
    time(&t);
    //sem_wait(print_mutex);
    printf("SIGINT recived\n");
    /*
    fprintf(log_file,"%s SIGINT recived\n",ctime(&t));
    fclose(config_file);
    pthread_mutex_lock(&queue_mutex);
    while (head->start!=NULL){
        ptr = head->start;
        head->start = ptr->next;
        free(ptr);
    }
    free(head);
    pthread_mutex_destroy(&queue_mutex);
    pthread_cond_destroy(&queue_full_cond);
    pthread_cond_destroy(&queue_empty_cond);
    close(sensor_fd);
    close(console_fd);
    unlink(SENSOR_PIPE);
    unlink(CONSOLE_PIPE);
    time(&t);*/
    printf("HOME_IOT SIMULATOR CLOSING\n");
    fprintf(log_file,"%s HOME_IOT SIMULATOR CLOSING\n",ctime(&t));

    sem_close(print_mutex);
    sem_close(shm_mutex);
    sem_close(available_workers_sem);
    sem_unlink("PRINT_MUTEX");
    sem_unlink("SHM_MUTEX");
    sem_unlink("AVAILABLE_WORKERS");

    fclose(log_file);
    exit(0);
}
void *dispatch(){
    time_t t;
    time(&t);
    sem_wait(print_mutex);
    printf("Dispatch thread starting!\n");
    fprintf(log_file,"%s Dispatch thread starting!\n",ctime(&t));
    sem_post(print_mutex);
    
    struct sigaction new;
    new.sa_handler = sigint_handler;
    new.sa_flags = 0;
    sigfillset(&new.sa_mask);
    sigaction(SIGINT,&new,NULL);
    char s[123];
    int x,y;

    while (1){
        pthread_mutex_lock(&queue_mutex);
        while (cur_queue_sz < 1){
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("QUEUE is empty! Nothing to pop!\n");
            sem_post(print_mutex);
            #endif
            pthread_cond_wait(&queue_empty_cond,&queue_mutex);
        }
        x = pop_queue(s);
        pthread_cond_signal(&queue_full_cond);
        pthread_mutex_unlock(&queue_mutex);
        if(x){
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("Removed from queue: %s\n",s);
            sem_post(print_mutex);
            #endif
            sem_wait(available_workers_sem);
            for (int i = 0; i < config[1]; i++){
                sem_wait(shm_mutex);//because of workers_array
                if(!mem->workers_array[i]){//if worker not occupied
                    mem->workers_array[i]=true;
                    sem_post(shm_mutex);
                    write(channels[i][1],s,strlen(s)+1);
                    break;
                }
                sem_post(shm_mutex);
                #ifdef DEBUG
                sem_wait(print_mutex);
                printf("Worker %d is occupied!\n",i);
                sem_post(print_mutex);
                #endif
            }
        }
    }
    //waitpid(-1,NULL,0);
    return NULL;
}
int main(int argc, char const *argv[]){
    log_file = fopen(LOG_FILE,"w");
    if(log_file == NULL){
        perror("log_file error");
        exit(0);
    }
    time_t t;
    time(&t);
    fprintf(log_file,"%s HOME_IOT SIMULATOR STARTING\n",ctime(&t));
    if (argc < 2){
        time(&t);
        printf("Erro, Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n");
        fprintf(log_file,"%s Erro, Sintaxe do comando a executar na linha de comando: $ home_iot {ficheiro de configuração}\n",ctime(&t));
        fclose(log_file);
        exit(0);
    }
    if(access(argv[1],R_OK)==0){//tests if the file exits and if it has read permissions
        config_file = fopen(argv[1],"r");
    }else{
        time(&t);
        perror("Config_file error");
        fprintf(log_file,"%s Config_file error: %s",ctime(&t),strerror(errno));
        fclose(log_file);
        exit(0);
    }
   
    char s[11];
    for (int i = 0; i < 5; i++){
        if(fgets(s,11,config_file)==NULL){
            time(&t);
            printf("Parâmetros insuficientes no ficheiro de configurações!\n");
            fprintf(log_file,"%s Parâmetros insuficientes no ficheiro de configurações!\n",ctime(&t));
            fclose(log_file);
            fclose(config_file);
            exit(0);
        }
        config[i]=atoi(s);
        if (config[i] < 1){//the last element of the config file (MAX_ALERTS) can be 0
            time(&t);
            if (i<4 || config[i]<0){//but cannot be less than 0
                printf("Existe um ou mais parâmetros incorretos!\n");
                fprintf(log_file,"%s Existe um ou mais parâmetros incorretos!\n",ctime(&t));
                fclose(log_file);
                fclose(config_file);
                exit(0);
            }
        }
        
    }
    
    if ((mkfifo(CONSOLE_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        time(&t);
        perror("Cannot create CONSOLE_PIPE");
        fprintf(log_file,"%s Cannot create CONSOLE_PIPE: %s\n",ctime(&t),strerror(errno));
        fclose(log_file);
        fclose(config_file);
        exit(0);
    }
    if ((mkfifo(SENSOR_PIPE, O_CREAT|O_EXCL|0600)<0) && (errno!= EEXIST)) {
        time(&t);
        perror("Cannot create SENSOR_PIPE");
        fprintf(log_file,"%s Cannot create SENSOR_PIPE: %s\n",ctime(&t),strerror(errno));
        fclose(log_file);
        fclose(config_file);
        exit(0);
    }
    
    cur_queue_sz = 0;

    head = (INTERNAL_QUEUE_HEAD*)malloc(sizeof(INTERNAL_QUEUE_HEAD));
    head->start = NULL;
    head->end = NULL;

    shmid = shmget(IPC_PRIVATE, sizeof(mem_struct)+config[1]*sizeof(bool)+config[2]*sizeof(sensor_struct)+config[3]*sizeof(sensors_list)+config[4]*sizeof(alert_struct), IPC_CREAT|0700);
    if (shmid < 1) exit(0);

    #ifdef DEBUG
    printf("Shared memory created!\n");
    #endif

    mem = (mem_struct*) shmat(shmid, NULL, 0);
    if (mem < (mem_struct*) 1) exit(0);
    
    #ifdef DEBUG
    printf("Shared memory attached!\n");
    #endif

    mem->workers_array = (bool*) (mem+1);
    mem->sensor_stats_array = (sensor_struct*)(mem->workers_array + config[1]);
    mem->sensors_array = (sensors_list*)(mem->sensor_stats_array + config[2]);
    mem->alerts_array = (alert_struct*)(mem->sensors_array + config[3]);

    mem->sensor_stats_count = mem->sensor_count = mem->alerts_count = 0;

    //strcpy(mem->sensors_array[0].id,"Hello");
    //strcpy(mem->sensors_array[1].id,"There");

    //printf("%d\n", mem->workers_array+1);
    //printf("%d\n",mem->sensor_stats_array+1);
    //printf("%s\n",mem->sensors_array[0].id);
    //printf("%s\n",mem->sensors_array[1].id);
    //printf("%d\n",mem->alerts_array+1);

    sem_unlink("AVAILABLE_WORKERS");
	available_workers_sem = sem_open("AVAILABLE_WORKERS",O_CREAT|O_EXCL,0700,config[1]);
	sem_unlink("SHM_MUTEX");
	shm_mutex =sem_open("SHM_MUTEX",O_CREAT|O_EXCL,0700,1);
	sem_unlink("PRINT_MUTEX");
	print_mutex =sem_open("PRINT_MUTEX",O_CREAT|O_EXCL,0700,1);

    __pid_t pid;
    
    for (int i = 0; i < config[1]; i++){
        pipe(channels[i]);
        pid = fork();
        if(pid == 0){
            print_mutex = sem_open("PRINT_MUTEX",0);
            shm_mutex = sem_open("SHM_MUTEX",0);
            available_workers_sem = sem_open("AVAILABLE_WORKERS",0);
            work(i);
            exit(0);
        }else if(pid ==-1){
            print(0,"Erro worker creation");
            exit(0);
        }
        close(channels[i][0]);
    }
    pid = fork();
    if(pid==0){
        alerts_watcher();
        exit(0);
    }else if(pid ==-1){
        print(0,"Erro worker creation");
        exit(0);
    }

    pthread_create(&console_reader,NULL,console,NULL);
    pthread_create(&sensor_reader,NULL,sensor,NULL);
    pthread_create(&dispatcher,NULL,dispatch,NULL);

    pthread_join(console_reader,NULL);
    pthread_join(sensor_reader,NULL);
    pthread_join(dispatcher,NULL);
    return 0;
}
