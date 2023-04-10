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
#include <sys/select.h>

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
    int my_shmid;
    struct sensors_list *next;
}sensors_list;
typedef struct sensor_struct{
    //char **key;
    char key[33];
    int last_value,min,max,updates,my_shmid;
    double average;
    struct sensor_struct *next;
}sensor_struct;
typedef struct alert_struct{
    char user_console_id[32];
    char key[32];
    int min,max,my_shmid;
    struct alert_struct *next;
}alert_struct;
typedef struct{
    sensor_struct *first_sensor_stats;
    alert_struct *first_alert;
    sensors_list *first_sensor;
    fd_set write_set;
} mem_struct;

int console_fd,sensor_fd,cur_queue_sz,cur_n_workers,cur_keys_n,cur_sensors_n,cur_alerts_n,shmid,channels[5][2];
/// @brief QUEUE_SZ, N_WORKERS, MAX_KEYS, MAX_SENSORS, MAX_ALERTS
int config[5];
pid_t *workers;
mem_struct* mem;
sem_t *available_workers,*shm_mutex,*print_mutex;
pthread_t sensor_reader,console_reader,dispatcher;
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t queue_full_cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t queue_empty_cond = PTHREAD_COND_INITIALIZER;
FILE *log_file,*config_file;


INTERNAL_QUEUE_HEAD *head;
fd_set *write_set;

void print(char s[]){
    sem_wait(print_mutex);
    printf("%s\n",s);
    printf("Printador por print\n");
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
    int aux_shmid;
    sensor_struct *aux_mem,*prev_aux_mem;
    sensors_list *aux_sensor,*prev_aux_sensor;
    id = strtok(s,car);
    aux_sensor = mem->first_sensor;
    prev_aux_sensor = aux_sensor;
    while (aux_sensor != NULL){
        prev_aux_sensor = aux_sensor;
        if(strcmp(id,aux_sensor->id)==0){//if id was found
            break;
        }
        aux_sensor = aux_sensor->next;
    }
    if(aux_sensor == NULL){//if it was not found
        aux_shmid = shmget(IPC_PRIVATE, sizeof(sensors_list), IPC_CREAT|0700);
        if (aux_shmid < 1) exit(0);
        aux_sensor = (sensors_list*) shmat(aux_shmid, NULL, 0);
        if (aux_sensor < (sensors_list*) 1) exit(0);
        aux_sensor->my_shmid = aux_shmid;
        strcpy(aux_sensor->id,id);
        aux_sensor->next = NULL;

        if(mem->first_sensor != NULL){
            prev_aux_sensor->next = aux_sensor;
        }else mem->first_sensor = aux_sensor;
    }
    key = strtok(0,car);
    value = strtok(0,car);
    aux_mem = mem->first_sensor_stats;
    prev_aux_mem = aux_mem;
    while (aux_mem != NULL){
        prev_aux_mem = aux_mem;
        if(strcmp(key,aux_mem->key)==0)break;
        aux_mem = aux_mem->next;
    }
    if(aux_mem!=NULL){
        aux_shmid = atoi(value);
        aux_mem->average = (aux_mem->updates * aux_mem->average + aux_shmid)/(aux_mem->updates + 1);
        aux_mem->last_value = aux_shmid;
        aux_mem->updates++;
        if(aux_shmid>aux_mem->max)aux_mem->max = aux_shmid;
        else if(aux_shmid < aux_mem->min)aux_mem->min = aux_shmid;
    }else{
        aux_shmid = shmget(IPC_PRIVATE, sizeof(sensor_struct), IPC_CREAT|0700);
        if (aux_shmid < 1) exit(0);
        aux_mem = (sensor_struct*) shmat(aux_shmid, NULL, 0);
        aux_mem->my_shmid = aux_shmid;
        if (aux_mem < (sensor_struct*) 1) exit(0);
        strcpy(aux_mem->key,key);
        aux_mem->average = aux_mem->last_value = aux_mem->max = aux_mem->min = atoi(value);
        aux_mem->updates = 1;
        aux_mem->my_shmid = aux_shmid;
        aux_mem->next = NULL;

        if(mem->first_sensor_stats != NULL){
            prev_aux_mem->next = aux_mem;
        }else mem->first_sensor_stats = aux_mem;
    }
}
void work_user(char s[]){
    char *s1;
    char car[]="#";
    s1 = strtok(s,car);
    s1 = strtok(0,car);
    if(strcmp(s1,"1")==0){
        sensor_struct *aux_mem;
        aux_mem = mem->first_sensor_stats;
        sem_wait(print_mutex);
        printf("Key Last Min Max Avg Count\n");
        while (aux_mem!=NULL){
            printf("%s %d %d %d %f %d\n",aux_mem->key,aux_mem->last_value,aux_mem->min,aux_mem->max,aux_mem->average,aux_mem->updates);
            aux_mem = aux_mem->next;
        }
        sem_post(print_mutex);
    }
}

void work(int worker_n){
    sem_wait(print_mutex);
    printf("worker %d starting\n",worker_n);
    sem_post(print_mutex);
    close(channels[worker_n][1]);
    char s[123];
    char c;
    int i;
    while (1){
        sem_wait(shm_mutex);
        FD_SET(channels[worker_n][1],write_set);
        read(channels[worker_n][0],s,sizeof(s));
        sem_post(shm_mutex);
        sem_wait(print_mutex);
        printf("WORKer %d : I've read: %s\n",worker_n,s);
        sem_post(print_mutex);
        //sleep(worker_n+3);
        i = 0;
        do{
            c = s[i];
            i++;
        } while (c!='#');
        i++;
        c = s[i];
        if(c!='#'){//Dados de um sensor
            /*sem_wait(print_mutex);
            printf("Worker a registar dados de sensor\n");
            sem_post(print_mutex);*/
            print("Worker a registar dados de sensor");
            sem_wait(shm_mutex);
            work_sensor(s);
            sem_post(shm_mutex);
        }else{//Dados de um user
            sem_wait(print_mutex);
            printf("Worker a registar dados de sensor\n");
            sem_post(print_mutex);
            sem_wait(shm_mutex);
            work_user(s);
            sem_post(shm_mutex);
        }
    }
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
        }
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
        }
    }
    return NULL;
}
void sigint_handler(){
    INTERNAL_QUEUE *ptr;
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
    sem_close(available_workers);
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
    
    for (int i = 0; i < config[1]; i++){
        pipe(channels[i]);
        if(fork()==0){
            work(i);
            exit(0);
        }
        close(channels[i][0]);
    }
    struct sigaction new;
    new.sa_handler = sigint_handler;
    new.sa_flags = 0;
    sigfillset(&new.sa_mask);
    sigaction(SIGINT,&new,NULL);
    char s[123];
    int x,y;

    FD_ZERO(write_set);
    for (int i = 0; i < config[1]; i++){
            sem_wait(shm_mutex);
            FD_SET(channels[i][1],write_set);
            sem_post(shm_mutex);
        }
    while (1){
        pthread_mutex_lock(&queue_mutex);
        while (cur_queue_sz < 1){
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
            //sleep(3);//to simulate worker working
                if(select(channels[config[1]-1]+1,NULL,write_set,NULL,NULL)>0){
                    for (int i = 0; i < config[1]; i++){
                        sem_wait(shm_mutex);
                        y = FD_ISSET(channels[i][1],write_set);
                        sem_post(shm_mutex);
                        if(y!=0){
                            //sem_wait(available_workers);
                            write(channels[i][1],s,strlen(s)+1);
                            sem_wait(shm_mutex);
                            FD_CLR(channels[i][1],write_set);
                            sem_post(shm_mutex);
                            break;
                        }
                        //sem_post(available_workers);
                        #ifdef DEBUG
                        sem_wait(print_mutex);
                        printf("Worker %d is occupied!\n",i);
                        sem_post(print_mutex);
                        #endif
                        
                    }
                }
            
        }else{
            #ifdef DEBUG
            sem_wait(print_mutex);
            printf("QUEUE is empty! Nothing to pop!\n");
            sem_post(print_mutex);
            #endif
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
    
    cur_queue_sz = cur_alerts_n = cur_keys_n = cur_n_workers = cur_sensors_n = 0;

    head = (INTERNAL_QUEUE_HEAD*)malloc(sizeof(INTERNAL_QUEUE_HEAD));
    head->start = NULL;
    head->end = NULL;

    /*shmid = shmget(IPC_PRIVATE, sizeof(fd_set), IPC_CREAT|0700);
    if (shmid < 1) exit(0);
    write_set = (fd_set*)shmat(shmid,NULL,0); 
    if (write_set < (fd_set*) 1) exit(0);*/

    shmid = shmget(IPC_PRIVATE, sizeof(mem_struct), IPC_CREAT|0700);
    if (shmid < 1) exit(0);

    #ifdef DEBUG
    printf("Shared memory created!\n");
    #endif

    mem = (mem_struct*) shmat(shmid, NULL, 0);
    if (mem < (mem_struct*) 1) exit(0);

    #ifdef DEBUG
    printf("Shared memory attached!\n");
    #endif

    /*sem_init(&mem->print_mutex,1,1);
    print_mutex = &mem->print_mutex;*/

    sem_unlink("AVAILABLE_WORKERS");
	available_workers = sem_open("AVAILABLE_WORKERS",O_CREAT|O_EXCL,0700,config[1]);
	sem_unlink("SHM_MUTEX");
	shm_mutex =sem_open("SHM_MUTEX",O_CREAT|O_EXCL,0700,1);
	sem_unlink("PRINT_MUTEX");
	print_mutex =sem_open("PRINT_MUTEX",O_CREAT|O_EXCL,0700,1);

    mem->first_alert = NULL;
    mem->first_sensor_stats = NULL;
    mem->first_sensor_stats = NULL;
    write_set = &mem->write_set;

    pthread_create(&console_reader,NULL,console,NULL);
    pthread_create(&sensor_reader,NULL,sensor,NULL);
    pthread_create(&dispatcher,NULL,dispatch,NULL);

    pthread_join(console_reader,NULL);
    pthread_join(sensor_reader,NULL);
    pthread_join(dispatcher,NULL);
    return 0;
}
